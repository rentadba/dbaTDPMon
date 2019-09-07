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
		@machineName			[sysname],
		@instanceID				[smallint],
		@queryToRun				[nvarchar](4000),
		@strMessage				[nvarchar](4000),
		@SQLMajorVersion		[int],
		@sqlServerVersion		[sysname],
		@runxpFixedDrives		[bit],
		@runwmicLogicalDisk		[bit],
		@errorCode				[int],
		@optionXPValue			[int], 
		@isAzureSQLDatabase		[bit]
		


/*-------------------------------------------------------------------------------------------------------------------------------*/
IF object_id('tempdb..#xpCMDShellOutput') IS NOT NULL 
DROP TABLE #xpCMDShellOutput

CREATE TABLE #xpCMDShellOutput
(
	[id]		[int] IDENTITY(1,1),
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
	[block_size]			[int]				NULL,
	[percent_available]		[numeric](6,2)		NULL
)

------------------------------------------------------------------------------------------------------------------------------------------
--get default projectCode
IF @projectCode IS NULL
	SET @projectCode = [dbo].[ufn_getProjectCode](NULL, NULL)

SELECT	@projectID = [id]
FROM	[dbo].[catalogProjects]
WHERE	[code] = @projectCode

IF @projectID IS NULL
	begin
		SET @strMessage=N'The value specifief for Project Code is not valid.'
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=1
	end


------------------------------------------------------------------------------------------------------------------------------------------
--
-------------------------------------------------------------------------------------------------------------------------
SET @strMessage='Step 1: Delete existing information....'
EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0

DELETE dsi
FROM [health-check].[statsDiskSpaceInfo]		dsi
INNER JOIN [dbo].[catalogInstanceNames] cin ON cin.[id] = dsi.[instance_id] AND cin.[project_id] = dsi.[project_id]
WHERE cin.[project_id] = @projectID
		AND cin.[name] LIKE @sqlServerNameFilter

DELETE lsam
FROM [dbo].[logAnalysisMessages]	lsam
INNER JOIN [dbo].[catalogInstanceNames] cin ON cin.[id] = lsam.[instance_id] AND cin.[project_id] = lsam.[project_id]
WHERE cin.[project_id] = @projectID
		AND cin.[name] LIKE @sqlServerNameFilter
		AND lsam.[descriptor]='dbo.usp_hcCollectDiskSpaceUsage'

-------------------------------------------------------------------------------------------------------------------------
SET @strMessage='Step 2: Get Instance Details Information....'
EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0
		
DECLARE crsActiveInstances CURSOR LOCAL FAST_FORWARD FOR 	SELECT	cin.[instance_id], cin.[instance_name], cin.[version], cin.[machine_name],
																	CASE WHEN cin.[engine] IN (5, 6) THEN 1 ELSE 0 END AS [isAzureSQLDatabase]
															FROM	[dbo].[vw_catalogInstanceNames] cin
															WHERE 	cin.[project_id] = @projectID
																	AND cin.[instance_active]=1
																	AND cin.[instance_name] LIKE @sqlServerNameFilter
															ORDER BY cin.[instance_name]
OPEN crsActiveInstances
FETCH NEXT FROM crsActiveInstances INTO @instanceID, @sqlServerName, @sqlServerVersion, @machineName, @isAzureSQLDatabase
WHILE @@FETCH_STATUS=0
	begin
		SET @strMessage='Analyzing server: ' + @sqlServerName
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 1, @messageTreelevel = 1, @stopExecution=0

		TRUNCATE TABLE #diskSpaceInfo
		TRUNCATE TABLE #xp_cmdshell
		TRUNCATE TABLE #xpCMDShellOutput

		BEGIN TRY
			SELECT @SQLMajorVersion = REPLACE(LEFT(ISNULL(@sqlServerVersion, ''), 2), '.', '') 
		END TRY
		BEGIN CATCH
			SET @SQLMajorVersion = 9
		END CATCH

		/* get volume space / free disk space details */
		IF @isAzureSQLDatabase = 0
			begin
				SET @runwmicLogicalDisk=1
				SET @runxpFixedDrives=1
				IF @SQLMajorVersion >= 10
					begin				
						IF @enableXPCMDSHELL=1
							begin
								SET  @optionXPValue	= 0

								/* enable xp_cmdshell configuration option */
								EXEC [dbo].[usp_changeServerOption_xp_cmdshell]   @serverToRun	 = @sqlServerName
																				, @flgAction	 = 1			-- 1=enable | 0=disable
																				, @optionXPValue = @optionXPValue OUTPUT
																				, @debugMode	 = @debugMode

								IF @optionXPValue = 0
									begin
										RETURN 1
									end										
							end

						/* get using powershell */
						SET @queryToRun = N''
						SET @queryToRun = @queryToRun + N'DECLARE @cmdQuery [varchar](1024); SET @cmdQuery=''powershell.exe -c "Get-WmiObject -ComputerName ''' + QUOTENAME(REPLACE(@machineName, '.workgroup', ''), '''') + ''' -Class Win32_Volume -Filter ''''DriveType = 3'''' | select name,capacity,freespace | foreach{$_.name+''''|''''+$_.capacity/1048576+''''%''''+$_.freespace/1048576+''''*''''}"''; EXEC xp_cmdshell @cmdQuery;'
			
						IF @sqlServerName<>@@SERVERNAME
							IF @SQLMajorVersion < 11
								SET @queryToRun = N'SELECT * FROM OPENQUERY([' + @sqlServerName + '], ''SET FMTONLY OFF; EXEC (''''' + REPLACE(@queryToRun, '''', '''''''''') + ''''')'')'
							ELSE
								SET @queryToRun = N'SELECT * FROM OPENQUERY([' + @sqlServerName + '], ''SET FMTONLY OFF; EXEC (''''' + REPLACE(@queryToRun, '''', '''''''''') + ''''') WITH RESULT SETS(([Output] [nvarchar](max)))'')'
						IF @debugMode = 1 EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0

						TRUNCATE TABLE #xpCMDShellOutput
						BEGIN TRY
							INSERT	INTO #xpCMDShellOutput([output])
									EXEC sp_executesql @queryToRun
						END TRY
						BEGIN CATCH
							IF @debugMode=1 
								begin 
									SET @strMessage='An error occured. It will be ignored: ' + ERROR_MESSAGE()					
									EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0
								end
						END CATCH

						--script to retrieve the values in MB from PS Script output
						INSERT	INTO #diskSpaceInfo([logical_drive], [volume_mount_point], [total_size_mb], [available_space_mb], [percent_available])
						SELECT    UPPER(SUBSTRING([volume_mount_point], 1, 1)) [logical_drive]
								, [volume_mount_point]
								, [total_size_mb]
								, [available_space_mb]
								, CAST(ISNULL(ROUND([available_space_mb] / CAST(NULLIF([total_size_mb], 0) AS [numeric](20,3)) * 100, 2), 0) AS [numeric](10,2)) AS [percent_available]
						FROM (
								SELECT	  RTRIM(LTRIM(SUBSTRING([output], 1, CHARINDEX('|', [output]) - 1))) AS [volume_mount_point]
										, ROUND(CAST(RTRIM(LTRIM(SUBSTRING([output], CHARINDEX('|', [output]) + 1, (CHARINDEX('%', [output]) - 1) - CHARINDEX('|', [output])) )) AS [float]),0) AS [total_size_mb]
										, ROUND(CAST(RTRIM(LTRIM(SUBSTRING([output], CHARINDEX('%', [output]) + 1, (CHARINDEX('*', [output]) - 1) - CHARINDEX('%', [output])) )) AS [float]),0) AS [available_space_mb]
								FROM #xpCMDShellOutput
								WHERE [output] LIKE '[A-Z][:]%'
							)x

						IF (SELECT COUNT(*) FROM #diskSpaceInfo) > 0
							begin
								SET @runwmicLogicalDisk=0
								SET @runxpFixedDrives=0
							end
						ELSE
							begin
								/* get using dmvs (slower for large number of databases) */
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
								IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0

								TRUNCATE TABLE #diskSpaceInfo
								BEGIN TRY
										INSERT	INTO #diskSpaceInfo([logical_drive], [volume_mount_point], [total_size_mb], [available_space_mb], [percent_available])
											EXEC sp_executesql @queryToRun
										SET @runwmicLogicalDisk=0
										SET @runxpFixedDrives=0
								END TRY
								BEGIN CATCH
									IF @debugMode=1 
										begin 
											SET @strMessage='An error occured. It will be ignored: ' + ERROR_MESSAGE()					
											EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0
										end
								END CATCH
						end
					end

				IF @runwmicLogicalDisk=1
					begin
						------------------------------------------------------------------------------------------------------------------------------------------
						IF @enableXPCMDSHELL=1
							begin
								SET  @optionXPValue	= 0

								/* enable xp_cmdshell configuration option */
								EXEC [dbo].[usp_changeServerOption_xp_cmdshell]   @serverToRun	 = @sqlServerName
																				, @flgAction	 = 1			-- 1=enable | 0=disable
																				, @optionXPValue = @optionXPValue OUTPUT
																				, @debugMode	 = @debugMode

								IF @optionXPValue = 0
									begin
										RETURN 1
									end										
							end

						/*-------------------------------------------------------------------------------------------------------------------------------*/
						/* try to run wmic */
						IF @enableXPCMDSHELL=1 AND @optionXPValue=1
							begin
								BEGIN TRY
										SET @queryToRun = N''
										SET @queryToRun = @queryToRun + N'DECLARE @cmdQuery [varchar](102); SET @cmdQuery=''wmic volume get Name, Capacity, FreeSpace, BlockSize, DriveType''; EXEC xp_cmdshell @cmdQuery;'
			
										IF @sqlServerName<>@@SERVERNAME
											SET @queryToRun = N'SELECT * FROM OPENQUERY([' + @sqlServerName + '], ''SET FMTONLY OFF; EXEC (''''' + REPLACE(@queryToRun, '''', '''''''''') + ''''')'')'
										IF @debugMode = 1 EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0

										INSERT	INTO #xpCMDShellOutput([output])
												EXEC sp_executesql @queryToRun

										DELETE FROM #xpCMDShellOutput WHERE LEN([output])<=3 OR [output] LIKE '%\\?\Volume%' OR [output] IS NULL

										INSERT	INTO #diskSpaceInfo([logical_drive], [volume_mount_point], [total_size_mb], [available_space_mb], [block_size])
												SELECT	  LEFT([name], 1) AS [drive]
														, LTRIM(RTRIM([name])) AS [name]
														, CAST(REPLACE([capacity], ' ', '') AS [bigint]) / (1024 * 1024.) AS [total_size_mb]
														, CAST(REPLACE([free_space], ' ', '') AS [bigint]) / (1024 * 1024.) AS [available_space_mb]
														, [block_size]
												FROM (
														SELECT SUBSTRING([output], [block_size_start_pos], [capacity_start_pos] - [block_size_start_pos] - 1)	 AS [block_size],
																SUBSTRING([output], [capacity_start_pos], [drive_type_start_pos] - [capacity_start_pos] - 1)	 AS [capacity],
																SUBSTRING([output], [drive_type_start_pos], [free_space_start_pos] - [drive_type_start_pos] - 1) AS [drive_type],
																SUBSTRING([output], [free_space_start_pos], [name_start_pos] - [free_space_start_pos] - 1)		 AS [free_space],
																SUBSTRING([output], [name_start_pos], LEN([output]) - [name_start_pos] - 1)						 AS [name]
														FROM #xpCMDShellOutput X
														INNER JOIN (
																	SELECT  CHARINDEX('BlockSize', [output]) AS [block_size_start_pos],
																			CHARINDEX('Capacity', [output])	 AS [capacity_start_pos],
																			CHARINDEX('DriveType', [output]) AS [drive_type_start_pos],
																			CHARINDEX('FreeSpace', [output]) AS [free_space_start_pos],
																			CHARINDEX('Name', [output])		 AS [name_start_pos]
																	FROM	#xpCMDShellOutput 
																	WHERE [id]=1
																	) P ON 1=1
														WHERE X.[id]>1
													)A
												WHERE [drive_type]=3

										DELETE FROM #diskSpaceInfo WHERE [total_size_mb]=0

										UPDATE #diskSpaceInfo
												SET [percent_available] =  CAST(ISNULL(ROUND([available_space_mb] / CAST(NULLIF([total_size_mb], 0) AS [numeric](20,3)) * 100, 2), 0) AS [numeric](10,2)) 

										SET @runxpFixedDrives=0
								END TRY
								BEGIN CATCH
									IF @debugMode=1 
										begin
											SET @strMessage = 'An error occured. It will be ignored: ' + ERROR_MESSAGE()					
											EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0
										end
								END CATCH
							end

						/*-------------------------------------------------------------------------------------------------------------------------------*/
						/* disable xp_cmdshell configuration option */
						EXEC [dbo].[usp_changeServerOption_xp_cmdshell]   @serverToRun	 = @sqlServerName
																		, @flgAction	 = 0			-- 1=enable | 0=disable
																		, @optionXPValue = @optionXPValue OUTPUT
																		, @debugMode	 = @debugMode
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

								IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0

								TRUNCATE TABLE #diskSpaceInfo
								BEGIN TRY
										INSERT	INTO #diskSpaceInfo([logical_drive], [available_space_mb])
											EXEC sp_executesql @queryToRun
								END TRY
								BEGIN CATCH
									SET @strMessage = ERROR_MESSAGE()
									EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0
									INSERT	INTO [dbo].[logAnalysisMessages]([instance_id], [project_id], [event_date_utc], [descriptor], [message])
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
									EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0

									INSERT	INTO [dbo].[logAnalysisMessages]([instance_id], [project_id], [event_date_utc], [descriptor], [message])
											SELECT  @instanceID
													, @projectID
													, GETUTCDATE()
													, 'dbo.usp_hcCollectDiskSpaceUsage'
													, @strMessage
								END CATCH
							end

					end
				end
		ELSE
			begin
				DECLARE @databaseName		[sysname],
						@elasticPoolName	[sysname],
						@queryParams		[nvarchar](512)
				
				/* get the linked server database name, if specified */
				SELECT @databaseName = ISNULL([catalog], 'master')
				FROM sys.servers
				WHERE UPPER([name]) = UPPER(@sqlServerName)

				/* check if database is part of an Elastic Pool or not */
				SET @queryToRun = N''
				SET @queryToRun = @queryToRun + N'SELECT ISNULL([elastic_pool_name], ''\'') AS [elastic_pool_name]
												FROM sys.database_service_objectives 
												WHERE [database_id] IN (
																		SELECT [database_id] 
																		FROM sys.databases WHERE [name]=''' + @databaseName + '''
																		)'
				SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
				SET @queryToRun = N'SELECT @elasticPoolName = [elastic_pool_name]
									FROM (' + @queryToRun + N')y'
				SET @queryParams = '@elasticPoolName [sysname] OUTPUT'
				IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0
				EXEC sp_executesql @queryToRun, @queryParams, @elasticPoolName = @elasticPoolName OUT

				IF @elasticPoolName = '\'
					begin		
						/* get Azure SQL Database max limits */
						SET @queryToRun = N''
						SET @queryToRun = @queryToRun + N'SELECT ''\'' AS [logical_drive]
																, DB_NAME() AS [volume_mount_point]
																, [total_size_mb]
																, CAST(([total_size_mb] - [space_used_mb]) AS [bigint]) AS [available_space_mb]
																, CAST(([total_size_mb] - [space_used_mb]) / [total_size_mb] * 100. AS [numeric](18,2)) AS [percent_available]
														FROM (		
																SELECT	CAST(DATABASEPROPERTYEX(''' + @databaseName + ''', ''MaxSizeInBytes'') AS [bigint]) /(1024 * 1024) AS [total_size_mb]
																		,[space_used_mb]
																FROM (
																		SELECT SUM([space_used_mb]) [space_used_mb]
																		FROM (
																				SELECT CAST(FILEPROPERTY([name], ''SpaceUsed'') AS [numeric](20,3)) * 8 / 1024. AS [space_used_mb]
																				FROM sys.database_files
																			)x
																	)y
															)z'

						SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
						IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0
				
						TRUNCATE TABLE #diskSpaceInfo
						BEGIN TRY
								INSERT	INTO #diskSpaceInfo([logical_drive], [volume_mount_point], [total_size_mb], [available_space_mb], [percent_available])
									EXEC sp_executesql @queryToRun
						END TRY
						BEGIN CATCH
							SET @strMessage = ERROR_MESSAGE()
							EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0
							INSERT	INTO [dbo].[logAnalysisMessages]([instance_id], [project_id], [event_date_utc], [descriptor], [message])
									SELECT  @instanceID
											, @projectID
											, GETUTCDATE()
											, 'dbo.usp_hcCollectDiskSpaceUsage'
											, @strMessage
						END CATCH
					end
						
				IF @databaseName IN ('master') 
					begin
						/* get Azure elastic pool / server limits */
						SET @queryToRun = N''
						SET @queryToRun = @queryToRun + N'SELECT ''\'' AS [logical_drive]
																, eprs.[elastic_pool_name] AS [volume_mount_point]
																, eprs.[elastic_pool_storage_limit_mb] AS [total_size_mb]
																, CAST((eprs.[elastic_pool_storage_limit_mb] * (100-eprs.[avg_allocated_storage_percent])/100.) AS [bigint]) AS [available_space_mb]
																, (100-eprs.[avg_allocated_storage_percent]) AS [percent_available]
														FROM sys.elastic_pool_resource_stats eprs
														INNER JOIN (
																	SELECT [elastic_pool_name],
																			MAX([end_time])		AS [end_time]
																	FROM sys.elastic_pool_resource_stats
																	GROUP BY [elastic_pool_name]
																   ) ep ON ep.[elastic_pool_name] = eprs.[elastic_pool_name]
																			AND ep.[end_time] = eprs.[end_time]'
						SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
						IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0
				
						BEGIN TRY
								INSERT	INTO #diskSpaceInfo([logical_drive], [volume_mount_point], [total_size_mb], [available_space_mb], [percent_available])
									EXEC sp_executesql @queryToRun
						END TRY
						BEGIN CATCH
							SET @strMessage = ERROR_MESSAGE()
							EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0
							INSERT	INTO [dbo].[logAnalysisMessages]([instance_id], [project_id], [event_date_utc], [descriptor], [message])
									SELECT  @instanceID
											, @projectID
											, GETUTCDATE()
											, 'dbo.usp_hcCollectDiskSpaceUsage'
											, @strMessage
						END CATCH
					end
			end

		/* save results to stats table */
		INSERT	INTO [health-check].[statsDiskSpaceInfo]([instance_id], [project_id], [event_date_utc], [logical_drive], [volume_mount_point], [total_size_mb], [available_space_mb], [percent_available], [block_size])
				SELECT    @instanceID, @projectID, GETUTCDATE()
						, [logical_drive], [volume_mount_point], [total_size_mb], [available_space_mb], [percent_available], [block_size]
				FROM #diskSpaceInfo
							
		FETCH NEXT FROM crsActiveInstances INTO @instanceID, @sqlServerName, @sqlServerVersion, @machineName, @isAzureSQLDatabase
	end
CLOSE crsActiveInstances
DEALLOCATE crsActiveInstances

/* for Azure SQL databases do a match and copy data if found */
INSERT	INTO [health-check].[statsDiskSpaceInfo]([instance_id], [project_id], [event_date_utc], [logical_drive], [volume_mount_point], [total_size_mb], [available_space_mb], [percent_available], [block_size])
SELECT	d.[instance_id], d.[project_id], GETUTCDATE(), s.[logical_drive], d.[volume_mount_point],
		s.[total_size_mb], s.[available_space_mb], s.[percent_available], s.[block_size]
FROM (
		SELECT cin.[project_id], cin.[instance_id], cin.[machine_name], sdd.[database_name], sdd.[volume_mount_point]
		FROM [dbo].[vw_catalogInstanceNames] cin
		INNER JOIN [health-check].[vw_statsDatabaseDetails] sdd ON sdd.[project_id] = cin.[project_id] AND sdd.[instance_id] = cin.[instance_id]
		LEFT JOIN [health-check].[vw_statsDiskSpaceInfo] sdsi ON sdsi.[project_id] = cin.[project_id] AND sdsi.[instance_id] = cin.[instance_id] AND sdsi.[volume_mount_point] = sdd.volume_mount_point
		WHERE cin.[engine] IN (5, 6)
			AND sdsi.[volume_mount_point] IS NULL
	)d
INNER JOIN
	(
		SELECT cin.[project_id], cin.[instance_id], cin.[machine_name], sdd.[database_name], sdd.[volume_mount_point], 
				sdsi.[logical_drive], sdsi.[total_size_mb], sdsi.[available_space_mb], sdsi.[percent_available], sdsi.[block_size]
		FROM [dbo].[vw_catalogInstanceNames] cin
		INNER JOIN [health-check].[vw_statsDatabaseDetails] sdd ON sdd.[project_id] = cin.[project_id] AND sdd.[instance_id] = cin.[instance_id]
		LEFT JOIN [health-check].[vw_statsDiskSpaceInfo] sdsi ON sdsi.[project_id] = cin.[project_id] AND sdsi.[instance_id] = cin.[instance_id] AND sdsi.[volume_mount_point] = sdd.volume_mount_point
		WHERE cin.[engine] IN (5, 6)
			AND sdsi.[volume_mount_point] IS NOT NULL
	)s ON d.[project_id] = s.[project_id] AND d.[machine_name] = s.[machine_name] AND d.[volume_mount_point] = s.[volume_mount_point] AND d.[database_name] = s.[database_name]
GO
