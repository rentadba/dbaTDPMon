USE [dbaTDPMon]
GO

RAISERROR('*-----------------------------------------------------------------------------*', 10, 1) WITH NOWAIT
RAISERROR('* dbaTDPMon (Troubleshoot Database Performance / Monitoring)                  *', 10, 1) WITH NOWAIT
RAISERROR('* https://github.com/rentadba/dbaTDPMon, under GNU (GPLv3) licence model      *', 10, 1) WITH NOWAIT
RAISERROR('*-----------------------------------------------------------------------------*', 10, 1) WITH NOWAIT
RAISERROR('* Patch script: from version 2017.6 to 2017.7 (2017.07.06)				  *', 10, 1) WITH NOWAIT
RAISERROR('*-----------------------------------------------------------------------------*', 10, 1) WITH NOWAIT

SELECT * FROM [dbo].[appConfigurations] WHERE [module] = 'common' AND [name] = 'Application Version'
GO
UPDATE [dbo].[appConfigurations] SET [value] = N'2017.07.06' WHERE [module] = 'common' AND [name] = 'Application Version'
GO


/*---------------------------------------------------------------------------------------------------------------------*/
/* patch module: health-check																						   */
/*---------------------------------------------------------------------------------------------------------------------*/
RAISERROR('Patching module: HEALTH-CHECK', 10, 1) WITH NOWAIT

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
--
-------------------------------------------------------------------------------------------------------------------------
RAISERROR('--Step 1: Delete existing information....', 10, 1) WITH NOWAIT

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
RAISERROR('--Step 2: Get Instance Details Information....', 10, 1) WITH NOWAIT
		
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
						EXEC [dbo].[usp_changeServerConfigurationOption]	@sqlServerName		= @sqlServerName,
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
								EXEC [dbo].[usp_changeServerConfigurationOption]	@sqlServerName		= @sqlServerName,
																					@configOptionName	= 'show advanced options',
																					@configOptionValue	= 1,
																					@optionIsAvailable	= @optionAdvancedIsAvailable OUT,
																					@optionCurrentValue	= @optionAdvancedValue OUT,
																					@optionHasChanged	= @optionAdvancedHasChanged OUT,
																					@executionLevel		= 0,
																					@debugMode			= @debugMode

								IF @optionAdvancedIsAvailable = 1 AND (@optionAdvancedValue=1 OR @optionAdvancedHasChanged=1)
									EXEC [dbo].[usp_changeServerConfigurationOption]	@sqlServerName		= @sqlServerName,
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

				/*-------------------------------------------------------------------------------------------------------------------------------*/
				/* try to run wmic */
				IF @enableXPCMDSHELL=1 AND @optionXPIsAvailable=1
					begin
						BEGIN TRY
								SET @queryToRun = N''
								SET @queryToRun = @queryToRun + N'DECLARE @cmdQuery [varchar](102); SET @cmdQuery=''wmic volume get Name, Capacity, FreeSpace, BlockSize, DriveType''; EXEC xp_cmdshell @cmdQuery;'
			
								IF @sqlServerName<>@@SERVERNAME
									SET @queryToRun = N'SELECT * FROM OPENQUERY([' + @sqlServerName + '], ''SET FMTONLY OFF; EXEC(''''' + REPLACE(@queryToRun, '''', '''''''''') + ''''')'')'
								IF @debugMode = 1 PRINT @queryToRun

								INSERT	INTO #xpCMDShellOutput([output])
										EXEC (@queryToRun)

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
							IF @debugMode=1 PRINT 'An error occured. It will be ignored: ' + ERROR_MESSAGE()					
						END CATCH
					end

				/*-------------------------------------------------------------------------------------------------------------------------------*/
				/* disable xp_cmdshell configuration option */
				IF @optionXPHasChanged = 1
					EXEC [dbo].[usp_changeServerConfigurationOption]	@sqlServerName		= @sqlServerName,
																		@configOptionName	= 'xp_cmdshell',
																		@configOptionValue	= 0,
																		@optionIsAvailable	= @optionXPIsAvailable OUT,
																		@optionCurrentValue	= @optionXPValue OUT,
																		@optionHasChanged	= @optionXPHasChanged OUT,
																		@executionLevel		= 0,
																		@debugMode			= @debugMode

				/* disable show advanced options configuration option */
				IF @optionAdvancedHasChanged = 1
						EXEC [dbo].[usp_changeServerConfigurationOption]	@sqlServerName		= @sqlServerName,
																			@configOptionName	= 'show advanced options',
																			@configOptionValue	= 0,
																			@optionIsAvailable	= @optionAdvancedIsAvailable OUT,
																			@optionCurrentValue	= @optionAdvancedValue OUT,
																			@optionHasChanged	= @optionAdvancedHasChanged OUT,
																			@executionLevel		= 0,
																			@debugMode			= @debugMode

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
							PRINT @strMessage

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
							
		FETCH NEXT FROM crsActiveInstances INTO @instanceID, @sqlServerName, @sqlServerVersion
	end
CLOSE crsActiveInstances
DEALLOCATE crsActiveInstances

GO

RAISERROR('Create procedure: [dbo].[usp_hcChangeFillFactorForIndexesFrequentlyFragmented]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (
	    SELECT * 
	      FROM sys.objects 
	     WHERE object_id = OBJECT_ID(N'[dbo].[usp_hcChangeFillFactorForIndexesFrequentlyFragmented]') 
	       AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[usp_hcChangeFillFactorForIndexesFrequentlyFragmented]
GO

CREATE PROCEDURE [dbo].[usp_hcChangeFillFactorForIndexesFrequentlyFragmented]
		@projectCode				[varchar](32)=NULL,
		@dropFillFactorByPercent	[tinyint] = 5,
		@minFillFactorAcceptedLevel	[tinyint] = 50,
		@executionLevel				[tinyint] = 0,
		@debugMode					[bit] = 0
/* WITH ENCRYPTION */
AS

-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 18.08.2015
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================

SET NOCOUNT ON

DECLARE	@minimumIndexMaintenanceFrequencyDays	[tinyint],
		@analyzeOnlyMessagesFromTheLastHours	[tinyint],
		@analyzeIndexMaintenanceOperation		[nvarchar](128),
		@affectedDependentObjects				[nvarchar](max),
		@instanceName							[sysname],
		@databaseName							[sysname],
		@tableSchema							[sysname],
		@tableName								[sysname],
		@indexName								[sysname],
		@fillFactor								[tinyint],
		@newFillFactor							[tinyint],
		@indexType								[sysname],
		@queryToRun								[nvarchar](max),
		@nestExecutionLevel						[tinyint],
		@errorCode								[int],
		@objectName								[nvarchar](512),
		@childObjectName						[sysname]
		
				
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


-----------------------------------------------------------------------------------------------------
SET @queryToRun=N'Analyzing event messages for frequently fragmented indexes...'
EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

DECLARE crsFrequentlyFragmentedIndexes CURSOR LOCAL FAST_FORWARD FOR	SELECT    [instance_name], [database_name]
																				, REPLACE(REPLACE(SUBSTRING([object_name], 1, CHARINDEX('].[', [object_name])), ']', ''), '[', '')						AS [schema_name]
																				, REPLACE(REPLACE(SUBSTRING([object_name], CHARINDEX('].[', [object_name])+2, LEN([object_name])), ']', ''), '[', '')	AS [table_name]
																				, REPLACE(REPLACE([index_name], ']', ''), '[', '') AS [index_name]
																				, CASE WHEN [fill_factor]=0 THEN 100 ELSE [fill_factor] END AS [fill_factor]
																				, [index_type]
																		FROM	[dbo].[ufn_hcGetIndexesFrequentlyFragmented](@projectCode, @minimumIndexMaintenanceFrequencyDays, @analyzeOnlyMessagesFromTheLastHours, @analyzeIndexMaintenanceOperation)
																		ORDER BY [instance_name], [database_name], [schema_name], [table_name], [index_name]
OPEN crsFrequentlyFragmentedIndexes
FETCH NEXT FROM crsFrequentlyFragmentedIndexes INTO @instanceName, @databaseName, @tableSchema, @tableName, @indexName, @fillFactor, @indexType
WHILE @@FETCH_STATUS=0
	begin
		--analyze curent object
		SET @queryToRun=N'instance=[' + @instanceName + '], database=[' + @databaseName + '], table-name=[' + @tableSchema + '].[' + @tableName + ']'
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 2, @stopExecution=0

		SET @queryToRun=N'index-name=[' + @indexName + '], type=[' + @indexType + '], current fill-factor= ' + CAST(@fillFactor AS [nvarchar])
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 3, @stopExecution=0
			
		SET @newFillFactor = @fillFactor-@dropFillFactorByPercent
		IF  @newFillFactor >= @minFillFactorAcceptedLevel
			begin
				SET @queryToRun=N'lowering fill-factor by ' + CAST(@dropFillFactorByPercent AS [nvarchar]) + ', new fill-factor value=' + CAST(@newFillFactor AS [nvarchar])
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 3, @stopExecution=0

				---------------------------------------------------------------------------------------------
				--get destination server running version/edition
				DECLARE		@serverEdition					[sysname],
							@serverVersionStr				[sysname],
							@serverVersionNum				[numeric](9,6),
							@nestedExecutionLevel			[tinyint]

				SET @nestedExecutionLevel = @executionLevel + 1
				EXEC [dbo].[usp_getSQLServerVersion]	@sqlServerName			= @instanceName,
														@serverEdition			= @serverEdition OUT,
														@serverVersionStr		= @serverVersionStr OUT,
														@serverVersionNum		= @serverVersionNum OUT,
														@executionLevel			= @nestedExecutionLevel,
														@debugMode				= @debugMode

				SET @nestExecutionLevel = @executionLevel + 3

				---------------------------------------------------------------------------------------------
				IF @serverVersionNum>=9
					begin
						EXEC [dbo].[usp_mpAlterTableIndexes]	@sqlServerName				= @instanceName,
																@dbName						= @databaseName,
																@tableSchema				= @tableSchema,
																@tableName					= @tableName,
																@indexName					= @indexName,
																@indexID					= NULL,
																@partitionNumber			= 1,
																@flgAction					= 1,
																@flgOptions					= DEFAULT,
																@maxDOP						= DEFAULT,
																@FillFactor					= @newFillFactor,
																@executionLevel				= 0,
																@affectedDependentObjects	= @affectedDependentObjects OUTPUT,
																@DebugMode					= @debugMode
					end
				ELSE
					begin
						SET @queryToRun = N'SET ARITHABORT ON; SET QUOTED_IDENTIFIER ON; '
						SET @queryToRun = @queryToRun +	N'IF OBJECT_ID(''[' + @tableSchema + '].[' + @tableName + ']'') IS NOT NULL DBCC DBREINDEX (''[' + @tableSchema + '].[' + @tableName + ']' + ''', ''' + RTRIM(@indexName) + ''', ' + CAST(@newFillFactor AS [nvarchar]) + N') WITH NO_INFOMSGS'
						IF @debugMode=1 EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

						SET @objectName = '[' + @tableSchema + '].[' + RTRIM(@tableName) + ']'
						SET @childObjectName = QUOTENAME(@indexName)

						EXEC @errorCode = [dbo].[usp_sqlExecuteAndLog]	@sqlServerName	= @instanceName,
																		@dbName			= @databaseName,
																		@objectName		= @objectName,
																		@childObjectName= @childObjectName,
																		@module			= 'dbo.usp_hcChangeFillFactorForIndexesFrequentlyFragmented',
																		@eventName		= 'database maintenance - rebuilding index',
																		@queryToRun  	= @queryToRun,
																		@flgOptions		= 0,
																		@executionLevel	= @nestedExecutionLevel,
																		@debugMode		= @debugMode						
					end
					
			end
		ELSE
			begin
				SET @queryToRun=N'fill factor will not be lowered, since it will be under acceptable limit = ' + CAST(@minFillFactorAcceptedLevel AS [nvarchar])
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 3, @stopExecution=0
			end
		FETCH NEXT FROM crsFrequentlyFragmentedIndexes INTO @instanceName, @databaseName, @tableSchema, @tableName, @indexName, @fillFactor, @indexType
	end
CLOSE crsFrequentlyFragmentedIndexes
DEALLOCATE crsFrequentlyFragmentedIndexes
GO


/*---------------------------------------------------------------------------------------------------------------------*/
USE [dbaTDPMon]
GO
SELECT * FROM [dbo].[appConfigurations] WHERE [module] = 'common' AND [name] = 'Application Version'
GO

RAISERROR('* Done *', 10, 1) WITH NOWAIT

