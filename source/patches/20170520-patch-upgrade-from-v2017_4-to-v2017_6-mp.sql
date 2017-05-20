USE [dbaTDPMon]
GO

RAISERROR('*-----------------------------------------------------------------------------*', 10, 1) WITH NOWAIT
RAISERROR('* dbaTDPMon (Troubleshoot Database Performance / Monitoring)                  *', 10, 1) WITH NOWAIT
RAISERROR('* https://github.com/rentadba/dbaTDPMon, under GNU (GPLv3) licence model      *', 10, 1) WITH NOWAIT
RAISERROR('*-----------------------------------------------------------------------------*', 10, 1) WITH NOWAIT
RAISERROR('* Patch script: from version 2017.4 to 2017.6 (2017.05.20)				  *', 10, 1) WITH NOWAIT
RAISERROR('*-----------------------------------------------------------------------------*', 10, 1) WITH NOWAIT

SELECT * FROM [dbo].[appConfigurations] WHERE [module] = 'common' AND [name] = 'Application Version'
GO
UPDATE [dbo].[appConfigurations] SET [value] = N'2017.05.20' WHERE [module] = 'common' AND [name] = 'Application Version'
GO

/*---------------------------------------------------------------------------------------------------------------------*/
/* patch module: commons																							   */
/*---------------------------------------------------------------------------------------------------------------------*/
RAISERROR('Patching module: COMMONS', 10, 1) WITH NOWAIT

RAISERROR('Create function: [dbo].[ufn_getTableFromStringList]', 10, 1) WITH NOWAIT
GO
if exists (select * from dbo.sysobjects where id = object_id(N'[dbo].[ufn_getTableFromStringList]') and xtype in (N'FN', N'IF', N'TF'))
drop function [dbo].[ufn_getTableFromStringList]
GO

SET QUOTED_IDENTIFIER ON 
GO
SET ANSI_NULLS ON 
GO

/* http://stackoverflow.com/questions/17481479/parse-comma-separated-string-to-make-in-list-of-strings-in-the-where-clause */
CREATE FUNCTION [dbo].[ufn_getTableFromStringList]
(
	@listWithValues		nvarchar(4000), 
	@valueDelimiter		char(1)
)
RETURNS @result TABLE 
	(	[id]	[int],
		[value] [nvarchar](4000)
	)
AS
begin
	SET @listWithValues = @listWithValues + @valueDelimiter
	;WITH lst AS
	(
		SELECT	CAST(1 AS [int]) StartPos, 
				CHARINDEX(@valueDelimiter, @listWithValues) EndPos, 
				1 AS [id]
		UNION ALL
		SELECT	EndPos + 1,
				CHARINDEX(@valueDelimiter, @listWithValues, EndPos + 1), 
				[id] + 1
		FROM lst
		WHERE CHARINDEX(@valueDelimiter, @listWithValues, EndPos + 1) > 0
	)
	INSERT @result ([id], [value])
	SELECT [id], SUBSTRING(@listWithValues, StartPos, EndPos - StartPos)
	FROM lst
	--OPTION (MAXRECURSION 0)

	RETURN
end
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
		@projectCode		[varchar](32)=NULL,
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
			SET @errMessage=N'ERROR: The value specifief for Project Code is not valid.'
			EXEC [dbo].[usp_logPrintMessage] @customMessage = @errMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=1
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
						BEGIN TRY
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
						END TRY
						BEGIN CATCH
							SET @queryToRun = N''
							SET @queryToRun = @queryToRun + N'SELECT DEFAULT_DOMAIN()';
							IF @sqlServerName<>@@SERVERNAME
							SET @queryToRun = N'SELECT * FROM OPENQUERY([' + @sqlServerName + '], ''SET FMTONLY OFF; EXEC(''''' + REPLACE(@queryToRun, '''', '''''''''') + ''''')'')'
							IF @debugMode = 1 PRINT @queryToRun
							INSERT	INTO #xpCMDShellOutput([output])
									EXEC (@queryToRun)
							SELECT TOP 1 @domainName = LOWER([output])
								FROM #xpCMDShellOutput
						END CATCH

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
/*
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
*/
	UPDATE dest
		SET dest.[domain]=src.[domain]
	FROM [dbo].[catalogMachineNames] AS dest
	INNER JOIN 
			(	
			 SELECT [name], [domain]
			 FROM #catalogMachineNames
			) src ON dest.[name] = src.[name] AND dest.[project_id] = @projectID;

	INSERT	INTO [dbo].[catalogMachineNames] ([project_id], [name], [domain]) 
			SELECT @projectID, src.[name], src.[domain]
			FROM 
				(	
					 SELECT [name], [domain]
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

	/*
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
	*/

	UPDATE dest
		SET	dest.[database_id] = src.[database_id]
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
		  ) AS src ON dest.[instance_id] = src.[instance_id] AND dest.[name] = src.[name];


	INSERT INTO [dbo].[catalogDatabaseNames]([instance_id], [project_id], [database_id], [name], [state], [state_desc], [active])
			SELECT src.[instance_id], @projectID, src.[database_id], src.[name], src.[state], src.[state_desc], 1
			FROM (	
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
				  ) AS src
			LEFT JOIN [dbo].[catalogDatabaseNames] AS dest ON dest.[instance_id] = src.[instance_id] AND dest.[name] = src.[name]
			WHERE dest.[instance_id] IS NULL;

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




/*---------------------------------------------------------------------------------------------------------------------*/
/* patch module: maintenance-plan																							   */
/*---------------------------------------------------------------------------------------------------------------------*/
RAISERROR('Patching module: MAINTENANCE-PLAN', 10, 1) WITH NOWAIT

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
		@flgOptions			[int] = 14360, --8192 + 4096 + 2048 + 16 + 8
		@MaxDOP				[smallint] = 1,
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
--					  4096  - rebuild/reorganize indexes/tables using ONLINE=ON, if applicable (default)
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
			@sqlScriptOnline			[nvarchar](512),
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
		DECLARE crsTableListToRebuild CURSOR LOCAL FAST_FORWARD FOR	SELECT [schema_name], [table_name], [record_count] 
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

								SET @sqlScriptOnline=N''

								-- check for online operation mode	
								IF @flgOptions & 4096 = 4096
									begin
										SET @nestedExecutionLevel = @executionLevel + 3
										EXEC [dbo].[usp_mpCheckIndexOnlineOperation]	@sqlServerName		= @SQLServerName,
																						@dbName				= @DBName,
																						@tableSchema		= @crtSchemaName,
																						@tableName			= @crtTableName,
																						@indexName			= NULL,
																						@indexID			= 0,
																						@partitionNumber	= 1,
																						@sqlScriptOnline	= @sqlScriptOnline OUT,
																						@flgOptions			= @flgOptions,
																						@executionLevel		= @nestedExecutionLevel,
																						@debugMode			= @DebugMode
									end

								IF (@flgOptions & 4096 = 4096) AND (@sqlScriptOnline LIKE N'ONLINE = ON%')
									begin
										SET @queryToRun=N'performing online table rebuild'
										SET @nestedExecutionLevel = @executionLevel + 2
										EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @nestedExecutionLevel, @messageTreelevel = 1, @stopExecution=0

										SET @queryToRun = N'IF OBJECT_ID(''' + @objectName + ''') IS NOT NULL ALTER TABLE ' + @objectName + N' REBUILD'
										SET @queryToRun = @queryToRun + N' WITH (' + @sqlScriptOnline + N'' + CASE WHEN ISNULL(@MaxDOP, 0) <> 0 THEN N', MAXDOP = ' + CAST(@MaxDOP AS [nvarchar]) ELSE N'' END + N')'
									end
								ELSE
									begin
										SET @queryToRun = N'SET LOCK_TIMEOUT ' + CAST(@queryLockTimeOut AS [nvarchar]) + N'; ';
										SET @queryToRun = @queryToRun + N'IF OBJECT_ID(''' + @objectName + ''') IS NOT NULL ALTER TABLE ' + @objectName + N' REBUILD' + CASE WHEN ISNULL(@MaxDOP, 0) <> 0 THEN N' WITH (MAXDOP = ' + CAST(@MaxDOP AS [nvarchar]) + N')' ELSE N'' END 
									end

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
																				@MaxDOP						= @MaxDOP,
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
																				@MaxDOP						= @MaxDOP,
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


RAISERROR('Create procedure: [dbo].[usp_mpJobQueueCreate]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (
	    SELECT * 
	      FROM sys.objects 
	     WHERE object_id = OBJECT_ID(N'[dbo].[usp_mpJobQueueCreate]') 
	       AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[usp_mpJobQueueCreate]
GO

CREATE PROCEDURE [dbo].[usp_mpJobQueueCreate]
		@projectCode			[varchar](32)=NULL,
		@module					[varchar](32)='maintenance-plan',
		@sqlServerNameFilter	[sysname]='%',
		@jobDescriptor			[varchar](256)='%',		/*	dbo.usp_mpDatabaseConsistencyCheck
															dbo.usp_mpDatabaseOptimize
															dbo.usp_mpDatabaseShrink
															dbo.usp_mpDatabaseBackup(Data)
															dbo.usp_mpDatabaseBackup(Log)
														*/
		@flgActions				[int] = 16383,			/*	   1	Weekly: Database Consistency Check - only once a week on Saturday
															   2	Daily: Allocation Consistency Check
															   4	Weekly: Tables Consistency Check - only once a week on Sunday
															   8	Weekly: Reference Consistency Check - only once a week on Sunday
															  16	Monthly: Perform Correction to Space Usage - on the first Saturday of the month
															  32	Daily: Rebuild Heap Tables - only for SQL versions +2K5
															  64	Daily: Rebuild or Reorganize Indexes
															 128	Daily: Update Statistics 
															 256	Weekly: Shrink Database (TRUNCATEONLY) - only once a week on Sunday
															 512	Monthly: Shrink Log File - on the first Saturday of the month 
															1024	Daily: Backup User Databases (diff) 
															2048	Weekly: User Databases (full) - only once a week on Saturday 
															4096	Weekly: System Databases (full) - only once a week on Saturday 
															8192	Hourly: Backup User Databases Transaction Log 
														*/
	    @recreateMode			[bit] = 1,				/*  1 - existings jobs will be dropped an created based on this stored procedure logic
															0 - jobs definition will be preserved; only status columns will be updated
														*/
		@debugMode				[bit] = 0
/* WITH ENCRYPTION */
AS

-- ============================================================================
-- Copyright (c) 2004-2016 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 23.08.2016
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================
SET NOCOUNT ON

DECLARE   @codeDescriptor		[varchar](260)
		, @strMessage			[varchar](1024)
		, @projectID			[smallint]
		, @instanceID			[smallint]
		, @featureflgActions	[int]
		, @forInstanceID		[int]
		, @forSQLServerName		[sysname]

DECLARE		@serverEdition					[sysname],
			@serverVersionStr				[sysname],
			@serverVersionNum				[numeric](9,6),
			@nestedExecutionLevel			[tinyint]

DECLARE @jobExecutionQueue TABLE
		(
			[instance_id]			[smallint]		NOT NULL,
			[project_id]			[smallint]		NOT NULL,
			[module]				[varchar](32)	NOT NULL,
			[descriptor]			[varchar](256)	NOT NULL,
			[for_instance_id]		[smallint]		NOT NULL,
			[job_name]				[sysname]		NOT NULL,
			[job_step_name]			[sysname]		NOT NULL,
			[job_database_name]		[sysname]		NOT NULL,
			[job_command]			[nvarchar](max) NOT NULL
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
		SET @strMessage=N'ERROR: The value specifief for Project Code is not valid.'
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=1
	end

------------------------------------------------------------------------------------------------------------------------------------------
SELECT @instanceID = [id]
FROM [dbo].[catalogInstanceNames]
WHERE [project_id] = @projectID
		AND [name] = @@SERVERNAME

------------------------------------------------------------------------------------------------------------------------------------------
DECLARE crsActiveInstances CURSOR LOCAL FAST_FORWARD FOR	SELECT	cin.[instance_id], cin.[instance_name]
															FROM	[dbo].[vw_catalogInstanceNames] cin
															WHERE 	cin.[project_id] = @projectID
																	AND cin.[instance_active]=1
																	AND cin.[instance_name] LIKE @sqlServerNameFilter
OPEN crsActiveInstances
FETCH NEXT FROM crsActiveInstances INTO @forInstanceID, @forSQLServerName
WHILE @@FETCH_STATUS=0
	begin
		SET @strMessage='--	Analyzing server: ' + @forSQLServerName
		RAISERROR(@strMessage, 10, 1) WITH NOWAIT

		--refresh current server information on internal metadata tables
		EXEC [dbo].[usp_refreshMachineCatalogs]	@projectCode	= @projectCode,
												@sqlServerName	= @forSQLServerName,
												@debugMode		= @debugMode


		--get destination server running version/edition
		SELECT @serverVersionNum = SUBSTRING([version], 1, CHARINDEX('.', [version])-1) + '.' + REPLACE(SUBSTRING([version], CHARINDEX('.', [version])+1, LEN([version])), '.', '')
		FROM	[dbo].[catalogInstanceNames]
		WHERE	[project_id] = @projectID
				AND [id] = @instanceID				

		DECLARE crsCollectorDescriptor CURSOR LOCAL FAST_FORWARD FOR	SELECT [descriptor]
																		FROM
																			(
																				SELECT 'dbo.usp_mpDatabaseConsistencyCheck' AS [descriptor] UNION ALL
																				SELECT 'dbo.usp_mpDatabaseOptimize' AS [descriptor] UNION ALL
																				SELECT 'dbo.usp_mpDatabaseShrink' AS [descriptor] UNION ALL
																				SELECT 'dbo.usp_mpDatabaseBackup(Data)' AS [descriptor] UNION ALL
																				SELECT 'dbo.usp_mpDatabaseBackup(Log)' AS [descriptor]
																			)X
																		WHERE (    [descriptor] LIKE @jobDescriptor
																				OR ISNULL(CHARINDEX([descriptor], @jobDescriptor), 0) <> 0
																				)			

		OPEN crsCollectorDescriptor
		FETCH NEXT FROM crsCollectorDescriptor INTO @codeDescriptor
		WHILE @@FETCH_STATUS=0
			begin
				SET @strMessage='Generating queue for : ' + @codeDescriptor
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 2, @stopExecution=0

				/* save the execution history */
				INSERT	INTO [dbo].[jobExecutionHistory]([instance_id], [project_id], [module], [descriptor], [filter], [for_instance_id], 
														 [job_name], [job_step_name], [job_database_name], [job_command], [execution_date], 
														 [running_time_sec], [log_message], [status], [event_date_utc])
						SELECT	[instance_id], [project_id], [module], [descriptor], [filter], [for_instance_id], 
								[job_name], [job_step_name], [job_database_name], [job_command], [execution_date], 
								[running_time_sec], [log_message], [status], [event_date_utc]
						FROM [dbo].[jobExecutionQueue]
						WHERE [project_id] = @projectID
								AND [instance_id] = @instanceID
								AND [descriptor] = @codeDescriptor
								AND [for_instance_id] = @forInstanceID 
								AND [module] = @module
								AND [status] <> -1

				IF @recreateMode = 1										
					DELETE FROM [dbo].[jobExecutionQueue]
					WHERE [project_id] = @projectID
							AND [instance_id] = @instanceID
							AND [descriptor] = @codeDescriptor
							AND [for_instance_id] = @forInstanceID 
							AND [module] = @module

				DELETE FROM @jobExecutionQueue

				------------------------------------------------------------------------------------------------------------------------------------------
				IF @codeDescriptor = 'dbo.usp_mpDatabaseConsistencyCheck'
					begin
						/*-------------------------------------------------------------------*/
						/* Weekly: Database Consistency Check - only once a week on Saturday */
						IF @flgActions & 1 = 1 AND [dbo].[ufn_mpCheckTaskSchedulerForDate](@projectCode, 'dbo.usp_mpDatabaseConsistencyCheck', 'Database Consistency Check', GETDATE()) = 1
								INSERT INTO @jobExecutionQueue (  [instance_id], [project_id], [module], [descriptor]
																, [for_instance_id], [job_name], [job_step_name], [job_database_name]
																, [job_command])
								SELECT	@instanceID AS [instance_id], @projectID AS [project_id], @module AS [module], @codeDescriptor AS [descriptor],
										@forInstanceID AS [for_instance_id], 
										SUBSTRING(DB_NAME() + ' - ' + 'dbo.usp_mpDatabaseConsistencyCheck' + ' - Database Consistency Check' + CASE WHEN @forSQLServerName <> @@SERVERNAME THEN ' - ' + REPLACE(@forSQLServerName, '\', '$') + ' ' ELSE ' - ' END + '[' + X.[database_name] + ']', 1, 128) AS [job_name],
										'Run'		AS [job_step_name],
										DB_NAME()	AS [job_database_name],
										'EXEC [dbo].[usp_mpDatabaseConsistencyCheck] @sqlServerName	= ''' + @forSQLServerName + N''', @dbName	= ''' + X.[database_name] + N''', @tableSchema = ''%'', @tableName = ''%'', @flgActions = 1, @flgOptions = 3, @debugMode = ' + CAST(@debugMode AS [varchar])
								FROM
									(
										SELECT [name] AS [database_name]
										FROM [dbo].[catalogDatabaseNames]
										WHERE	[project_id] = @projectID
												AND [instance_id] = @forInstanceID
												AND [active] = 1
												AND [name] NOT IN ('master', 'model', 'msdb', 'tempdb', 'distribution')														
												AND [state_desc] IN  ('ONLINE', 'READ ONLY')
									)X

						/*-------------------------------------------------------------------*/
						/* Daily: Allocation Consistency Check */
						/* when running DBCC CHECKDB, skip running DBCC CHECKALLOC*/
						IF [dbo].[ufn_mpCheckTaskSchedulerForDate](@projectCode, 'dbo.usp_mpDatabaseConsistencyCheck', 'Database Consistency Check', GETDATE()) = 1
							SET @featureflgActions = 8
						ELSE
							SET @featureflgActions = 12

						IF @flgActions & 2 = 2 AND [dbo].[ufn_mpCheckTaskSchedulerForDate](@projectCode, 'dbo.usp_mpDatabaseConsistencyCheck', 'Allocation Consistency Check', GETDATE()) = 1
								INSERT INTO @jobExecutionQueue (  [instance_id], [project_id], [module], [descriptor]
																, [for_instance_id], [job_name], [job_step_name], [job_database_name]
																, [job_command])
								SELECT	@instanceID AS [instance_id], @projectID AS [project_id], @module AS [module], @codeDescriptor AS [descriptor],
										@forInstanceID AS [for_instance_id], 
										SUBSTRING(DB_NAME() + ' - ' + 'dbo.usp_mpDatabaseConsistencyCheck' + ' - Allocation Consistency Check' + CASE WHEN @forSQLServerName <> @@SERVERNAME THEN ' - ' + REPLACE(@forSQLServerName, '\', '$') + ' ' ELSE ' - ' END + '[' + X.[database_name] + ']', 1, 128) AS [job_name],
										'Run'		AS [job_step_name],
										DB_NAME()	AS [job_database_name],
										'EXEC [dbo].[usp_mpDatabaseConsistencyCheck] @sqlServerName = ''' + @forSQLServerName + N''', @dbName = ''' + X.[database_name] + N''', @tableSchema = ''%'', @tableName = ''%'', @flgActions = ' + CAST(@featureflgActions AS [nvarchar]) + N', @flgOptions = DEFAULT, @debugMode = ' + CAST(@debugMode AS [varchar])
								FROM
									(
										SELECT [name] AS [database_name]
										FROM [dbo].[catalogDatabaseNames]
										WHERE	[project_id] = @projectID
												AND [instance_id] = @forInstanceID
												AND [active] = 1
												AND [name] NOT IN ('master', 'model', 'msdb', 'tempdb', 'distribution')														
												AND [state_desc] IN  ('ONLINE', 'READ ONLY')
									)X

						/*-------------------------------------------------------------------*/
						/* Weekly: Tables Consistency Check - only once a week on Sunday*/
						IF @flgActions & 4 = 4 AND [dbo].[ufn_mpCheckTaskSchedulerForDate](@projectCode, 'dbo.usp_mpDatabaseConsistencyCheck', 'Tables Consistency Check', GETDATE()) = 1
								INSERT INTO @jobExecutionQueue (  [instance_id], [project_id], [module], [descriptor]
																, [for_instance_id], [job_name], [job_step_name], [job_database_name]
																, [job_command])
								SELECT	@instanceID AS [instance_id], @projectID AS [project_id], @module AS [module], @codeDescriptor AS [descriptor],
										@forInstanceID AS [for_instance_id], 
										SUBSTRING(DB_NAME() + ' - ' + 'dbo.usp_mpDatabaseConsistencyCheck' + ' - Tables Consistency Check' + CASE WHEN @forSQLServerName <> @@SERVERNAME THEN ' - ' + REPLACE(@forSQLServerName, '\', '$') + ' ' ELSE ' - ' END + '[' + X.[database_name] + ']', 1, 128) AS [job_name],
										'Run'		AS [job_step_name],
										DB_NAME()	AS [job_database_name],
										'EXEC [dbo].[usp_mpDatabaseConsistencyCheck] @sqlServerName = ''' + @forSQLServerName + N''', @dbName	= ''' + X.[database_name] + N''', @tableSchema = ''%'', @tableName = ''%'', @flgActions = 2, @flgOptions = DEFAULT, @debugMode = ' + CAST(@debugMode AS [varchar])
								FROM
									(
										SELECT [name] AS [database_name]
										FROM [dbo].[catalogDatabaseNames]
										WHERE	[project_id] = @projectID
												AND [instance_id] = @forInstanceID
												AND [active] = 1
												AND [name] NOT IN ('master', 'model', 'msdb', 'tempdb', 'distribution')														
												AND [state_desc] IN  ('ONLINE', 'READ ONLY')
									)X

						/*-------------------------------------------------------------------*/
						/* Weekly: Reference Consistency Check - only once a week on Sunday*/
						IF @flgActions & 8 = 8 AND [dbo].[ufn_mpCheckTaskSchedulerForDate](@projectCode, 'dbo.usp_mpDatabaseConsistencyCheck', 'Reference Consistency Check', GETDATE()) = 1
								INSERT INTO @jobExecutionQueue (  [instance_id], [project_id], [module], [descriptor]
																, [for_instance_id], [job_name], [job_step_name], [job_database_name]
																, [job_command])
								SELECT	@instanceID AS [instance_id], @projectID AS [project_id], @module AS [module], @codeDescriptor AS [descriptor],
										@forInstanceID AS [for_instance_id], 
										SUBSTRING(DB_NAME() + ' - ' + 'dbo.usp_mpDatabaseConsistencyCheck' + ' - Reference Consistency Check' + CASE WHEN @forSQLServerName <> @@SERVERNAME THEN ' - ' + REPLACE(@forSQLServerName, '\', '$') + ' ' ELSE ' - ' END + '[' + X.[database_name] + ']', 1, 128) AS [job_name],
										'Run'		AS [job_step_name],
										DB_NAME()	AS [job_database_name],
										'EXEC [dbo].[usp_mpDatabaseConsistencyCheck] @sqlServerName = ''' + @forSQLServerName + N''', @dbName = ''' + X.[database_name] + N''', @tableSchema = ''%'', @tableName = ''%'', @flgActions = 16, @flgOptions = DEFAULT, @debugMode = ' + CAST(@debugMode AS [varchar])
								FROM
									(
										SELECT [name] AS [database_name]
										FROM [dbo].[catalogDatabaseNames]
										WHERE	[project_id] = @projectID
												AND [instance_id] = @forInstanceID
												AND [active] = 1
												AND [name] NOT IN ('master', 'model', 'msdb', 'tempdb', 'distribution')														
												AND [state_desc] IN  ('ONLINE', 'READ ONLY')
									)X

						/*-------------------------------------------------------------------*/
						/* Monthly: Perform Correction to Space Usage - on the first Saturday of the month */
						IF @flgActions & 16 = 16 AND [dbo].[ufn_mpCheckTaskSchedulerForDate](@projectCode, 'dbo.usp_mpDatabaseConsistencyCheck', 'Perform Correction to Space Usage', GETDATE()) = 1
								INSERT INTO @jobExecutionQueue (  [instance_id], [project_id], [module], [descriptor]
																, [for_instance_id], [job_name], [job_step_name], [job_database_name]
																, [job_command])
								SELECT	@instanceID AS [instance_id], @projectID AS [project_id], @module AS [module], @codeDescriptor AS [descriptor],
										@forInstanceID AS [for_instance_id], 
										SUBSTRING(DB_NAME() + ' - ' + 'dbo.usp_mpDatabaseConsistencyCheck' + ' - Perform Correction to Space Usage' + CASE WHEN @forSQLServerName <> @@SERVERNAME THEN ' - ' + REPLACE(@forSQLServerName, '\', '$') + ' ' ELSE ' - ' END + '[' + X.[database_name] + ']', 1, 128) AS [job_name],
										'Run'		AS [job_step_name],
										DB_NAME()	AS [job_database_name],
										'EXEC [dbo].[usp_mpDatabaseConsistencyCheck] @sqlServerName = ''' + @forSQLServerName + N''', @dbName = ''' + X.[database_name] + N''', @tableSchema = ''%'', @tableName = ''%'', @flgActions = 64, @flgOptions = DEFAULT, @debugMode = ' + CAST(@debugMode AS [varchar])
								FROM
									(
										SELECT [name] AS [database_name]
										FROM [dbo].[catalogDatabaseNames]
										WHERE	[project_id] = @projectID
												AND [instance_id] = @forInstanceID
												AND [active] = 1
												AND [name] NOT IN ('master', 'model', 'msdb', 'tempdb', 'distribution')														
												AND [state_desc] IN  ('ONLINE')
									)X
					end


				------------------------------------------------------------------------------------------------------------------------------------------
				IF @codeDescriptor = 'dbo.usp_mpDatabaseOptimize'
					begin
						/*-------------------------------------------------------------------*/
						/* Daily: Rebuild Heap Tables - only for SQL versions +2K5*/
						IF @flgActions & 32 = 32 AND @serverVersionNum > 9 AND [dbo].[ufn_mpCheckTaskSchedulerForDate](@projectCode, 'dbo.usp_mpDatabaseOptimize', 'Rebuild Heap Tables', GETDATE()) = 1
								INSERT INTO @jobExecutionQueue (  [instance_id], [project_id], [module], [descriptor]
																, [for_instance_id], [job_name], [job_step_name], [job_database_name]
																, [job_command])
								SELECT	@instanceID AS [instance_id], @projectID AS [project_id], @module AS [module], @codeDescriptor AS [descriptor],
										@forInstanceID AS [for_instance_id], 
										SUBSTRING(DB_NAME() + ' - ' + 'dbo.usp_mpDatabaseOptimize' + ' - Rebuild Heap Tables' + CASE WHEN @forSQLServerName <> @@SERVERNAME THEN ' - ' + REPLACE(@forSQLServerName, '\', '$') + ' ' ELSE ' - ' END + '[' + X.[database_name] + ']', 1, 128) AS [job_name],
										'Run'		AS [job_step_name],
										DB_NAME()	AS [job_database_name],
										'EXEC [dbo].[usp_mpDatabaseOptimize] @SQLServerName = ''' + @forSQLServerName + N''', @DBName = ''' + X.[database_name] + N''', @TableSchema = ''%'', @TableName = ''%'', @flgActions = 16, @flgOptions = DEFAULT, @DefragIndexThreshold = DEFAULT, @RebuildIndexThreshold = DEFAULT, @MaxDOP = DEFAULT, @skipObjectsList = DEFAULT, @DebugMode = ' + CAST(@debugMode AS [varchar])
								FROM
									(
										SELECT [name] AS [database_name]
										FROM [dbo].[catalogDatabaseNames]
										WHERE	[project_id] = @projectID
												AND [instance_id] = @forInstanceID
												AND [active] = 1
												AND [name] NOT IN ('master', 'model', 'msdb', 'tempdb', 'distribution')														
												AND [state_desc] IN  ('ONLINE')
									)X

						/*-------------------------------------------------------------------*/
						/* Daily: Rebuild or Reorganize Indexes*/			
						IF @flgActions & 64 = 64 AND [dbo].[ufn_mpCheckTaskSchedulerForDate](@projectCode, 'dbo.usp_mpDatabaseOptimize', 'Rebuild or Reorganize Indexes', GETDATE()) = 1
							begin
								SET @featureflgActions = 3
								
								IF @flgActions & 128 = 128 AND [dbo].[ufn_mpCheckTaskSchedulerForDate](@projectCode, 'dbo.usp_mpDatabaseOptimize', 'Update Statistics', GETDATE()) = 1 /* Daily: Update Statistics */
									SET @featureflgActions = 11

								INSERT INTO @jobExecutionQueue (  [instance_id], [project_id], [module], [descriptor]
																, [for_instance_id], [job_name], [job_step_name], [job_database_name]
																, [job_command])
								SELECT	@instanceID AS [instance_id], @projectID AS [project_id], @module AS [module], @codeDescriptor AS [descriptor],
										@forInstanceID AS [for_instance_id], 
										SUBSTRING(DB_NAME() + ' - ' + 'dbo.usp_mpDatabaseOptimize' + ' - Rebuild or Reorganize Indexes' + CASE WHEN @forSQLServerName <> @@SERVERNAME THEN ' - ' + REPLACE(@forSQLServerName, '\', '$') + ' ' ELSE ' - ' END + '[' + X.[database_name] + ']', 1, 128) AS [job_name],
										'Run'		AS [job_step_name],
										DB_NAME()	AS [job_database_name],
										'EXEC [dbo].[usp_mpDatabaseOptimize] @SQLServerName = ''' + @forSQLServerName + N''', @DBName = ''' + X.[database_name] + N''', @TableSchema = ''%'', @TableName = ''%'', @flgActions = ' + CAST(@featureflgActions AS [varchar]) + ', @flgOptions = DEFAULT, @DefragIndexThreshold = DEFAULT, @RebuildIndexThreshold = DEFAULT, @MaxDOP = DEFAULT, @skipObjectsList = DEFAULT, @DebugMode = ' + CAST(@debugMode AS [varchar])
								FROM
									(
										SELECT [name] AS [database_name]
										FROM [dbo].[catalogDatabaseNames]
										WHERE	[project_id] = @projectID
												AND [instance_id] = @forInstanceID
												AND [active] = 1
												AND [name] NOT IN ('master', 'model', 'msdb', 'tempdb', 'distribution')														
												AND [state_desc] IN  ('ONLINE')
									)X
							end

						/*-------------------------------------------------------------------*/
						/* Daily: Update Statistics */
						IF @flgActions & 128 = 128 AND NOT (@flgActions & 64 = 64) AND [dbo].[ufn_mpCheckTaskSchedulerForDate](@projectCode, 'dbo.usp_mpDatabaseOptimize', 'Update Statistics', GETDATE()) = 1
								INSERT INTO @jobExecutionQueue (  [instance_id], [project_id], [module], [descriptor]
																, [for_instance_id], [job_name], [job_step_name], [job_database_name]
																, [job_command])
								SELECT	@instanceID AS [instance_id], @projectID AS [project_id], @module AS [module], @codeDescriptor AS [descriptor],
										@forInstanceID AS [for_instance_id], 
										SUBSTRING(DB_NAME() + ' - ' + 'dbo.usp_mpDatabaseOptimize' + ' - Update Statistics' + CASE WHEN @forSQLServerName <> @@SERVERNAME THEN ' - ' + REPLACE(@forSQLServerName, '\', '$') + ' ' ELSE ' - ' END + '[' + X.[database_name] + ']', 1, 128) AS [job_name],
										'Run'		AS [job_step_name],
										DB_NAME()	AS [job_database_name],
										'EXEC [dbo].[usp_mpDatabaseOptimize] @SQLServerName = ''' + @forSQLServerName + N''', @DBName = ''' + X.[database_name] + N''', @TableSchema = ''%'', @TableName = ''%'', @flgActions = 8, @flgOptions = DEFAULT, @DefragIndexThreshold = DEFAULT, @RebuildIndexThreshold = DEFAULT, @MaxDOP = DEFAULT, @skipObjectsList = DEFAULT, @DebugMode = ' + CAST(@debugMode AS [varchar])
								FROM
									(
										SELECT [name] AS [database_name]
										FROM [dbo].[catalogDatabaseNames]
										WHERE	[project_id] = @projectID
												AND [instance_id] = @forInstanceID
												AND [active] = 1
												AND [name] NOT IN ('master', 'model', 'msdb', 'tempdb', 'distribution')														
												AND [state_desc] IN  ('ONLINE')
									)X
					end

				------------------------------------------------------------------------------------------------------------------------------------------
				IF @codeDescriptor = 'dbo.usp_mpDatabaseShrink'
					begin
						/*-------------------------------------------------------------------*/
						/* Weekly: Shrink Database (TRUNCATEONLY) - only once a week on Sunday*/
						IF @flgActions & 256 = 256 AND [dbo].[ufn_mpCheckTaskSchedulerForDate](@projectCode, 'dbo.usp_mpDatabaseShrink', 'Shrink Database (TRUNCATEONLY)', GETDATE()) = 1
								INSERT INTO @jobExecutionQueue (  [instance_id], [project_id], [module], [descriptor]
																, [for_instance_id], [job_name], [job_step_name], [job_database_name]
																, [job_command])
								SELECT	@instanceID AS [instance_id], @projectID AS [project_id], @module AS [module], @codeDescriptor AS [descriptor],
										@forInstanceID AS [for_instance_id], 
										SUBSTRING(DB_NAME() + ' - ' + 'dbo.usp_mpDatabaseOptimize' + ' - Shrink Database (TRUNCATEONLY)' + CASE WHEN @forSQLServerName <> @@SERVERNAME THEN ' - ' + REPLACE(@forSQLServerName, '\', '$') + ' ' ELSE ' - ' END + '[' + X.[database_name] + ']', 1, 128) AS [job_name],
										'Run'		AS [job_step_name],
										DB_NAME()	AS [job_database_name],
										'EXEC [dbo].[usp_mpDatabaseShrink] @SQLServerName = ''' + @forSQLServerName + N''', @DBName = ''' + X.[database_name] + N''', @flgActions = 2, @flgOptions = 1, @executionLevel = DEFAULT, @DebugMode = ' + CAST(@debugMode AS [varchar])
								FROM
									(
										SELECT [name] AS [database_name]
										FROM [dbo].[catalogDatabaseNames]
										WHERE	[project_id] = @projectID
												AND [instance_id] = @forInstanceID
												AND [active] = 1
												AND [name] NOT IN ('master', 'model', 'msdb', 'tempdb', 'distribution')														
												AND [state_desc] IN  ('ONLINE')
									)X

						/*-------------------------------------------------------------------*/
						/* Monthly: Shrink Log File - on the first Saturday of the month */
						IF @flgActions & 512 = 512 AND [dbo].[ufn_mpCheckTaskSchedulerForDate](@projectCode, 'dbo.usp_mpDatabaseShrink', 'Shrink Log File', GETDATE()) = 1
								INSERT INTO @jobExecutionQueue (  [instance_id], [project_id], [module], [descriptor]
																, [for_instance_id], [job_name], [job_step_name], [job_database_name]
																, [job_command])
								SELECT	@instanceID AS [instance_id], @projectID AS [project_id], @module AS [module], @codeDescriptor AS [descriptor],
										@forInstanceID AS [for_instance_id], 
										SUBSTRING(DB_NAME() + ' - ' + 'dbo.usp_mpDatabaseOptimize' + ' - Shrink Log File' + CASE WHEN @forSQLServerName <> @@SERVERNAME THEN ' - ' + REPLACE(@forSQLServerName, '\', '$') + ' ' ELSE ' - ' END + '[' + X.[database_name] + ']', 1, 128) AS [job_name],
										'Run'		AS [job_step_name],
										DB_NAME()	AS [job_database_name],
										'EXEC [dbo].[usp_mpDatabaseShrink] @SQLServerName = ''' + @forSQLServerName + N''', @DBName = ''' + X.[database_name] + N''', @flgActions = 1, @flgOptions = 0, @executionLevel = DEFAULT, @DebugMode = ' + CAST(@debugMode AS [varchar])
								FROM
									(
										SELECT [name] AS [database_name]
										FROM [dbo].[catalogDatabaseNames]
										WHERE	[project_id] = @projectID
												AND [instance_id] = @forInstanceID
												AND [active] = 1
												AND [name] NOT IN ('master', 'model', 'msdb', 'tempdb', 'distribution')														
												AND [state_desc] IN  ('ONLINE')
									)X

					end

				------------------------------------------------------------------------------------------------------------------------------------------
				IF @codeDescriptor = 'dbo.usp_mpDatabaseBackup(Data)'
					begin
						/*-------------------------------------------------------------------*/
						/* Daily: Backup User Databases (diff) */
						IF @flgActions & 1024 = 1024 AND [dbo].[ufn_mpCheckTaskSchedulerForDate](@projectCode, 'dbo.usp_mpDatabaseBackup', 'User Databases (diff)', GETDATE()) = 1
							AND NOT (@flgActions & 2048 = 2048 AND [dbo].[ufn_mpCheckTaskSchedulerForDate](@projectCode, 'dbo.usp_mpDatabaseBackup', 'User Databases (full)', GETDATE()) = 1)
								INSERT INTO @jobExecutionQueue (  [instance_id], [project_id], [module], [descriptor]
																, [for_instance_id], [job_name], [job_step_name], [job_database_name]
																, [job_command])
								SELECT	@instanceID AS [instance_id], @projectID AS [project_id], @module AS [module], @codeDescriptor AS [descriptor],
										@forInstanceID AS [for_instance_id], 
										SUBSTRING(DB_NAME() + ' - ' + 'dbo.usp_mpDatabaseBackup' + ' - Backup User Databases (diff)' + CASE WHEN @forSQLServerName <> @@SERVERNAME THEN ' - ' + REPLACE(@forSQLServerName, '\', '$') + ' ' ELSE ' - ' END + '[' + X.[database_name] + ']', 1, 128) AS [job_name],
										'Run'		AS [job_step_name],
										DB_NAME()	AS [job_database_name],
										'EXEC [dbo].[usp_mpDatabaseBackup] @sqlServerName = ''' + @forSQLServerName + N''', @dbName = ''' + X.[database_name] + N''', @backupLocation = DEFAULT, @flgActions = 2, @flgOptions = DEFAULT,	@retentionDays = DEFAULT, @executionLevel = DEFAULT, @debugMode = ' + CAST(@debugMode AS [varchar])
								FROM
									(
										SELECT [name] AS [database_name]
										FROM [dbo].[catalogDatabaseNames]
										WHERE	[project_id] = @projectID
												AND [instance_id] = @forInstanceID
												AND [active] = 1
												AND [name] NOT IN ('master', 'model', 'msdb', 'tempdb', 'distribution')														
									)X

						/*-------------------------------------------------------------------*/
						/* Weekly: User Databases (full) - only once a week on Saturday */
						IF @flgActions & 2048 = 2048 AND [dbo].[ufn_mpCheckTaskSchedulerForDate](@projectCode, 'dbo.usp_mpDatabaseBackup', 'User Databases (full)', GETDATE()) = 1
								INSERT INTO @jobExecutionQueue (  [instance_id], [project_id], [module], [descriptor]
																, [for_instance_id], [job_name], [job_step_name], [job_database_name]
																, [job_command])
								SELECT	@instanceID AS [instance_id], @projectID AS [project_id], @module AS [module], @codeDescriptor AS [descriptor],
										@forInstanceID AS [for_instance_id], 
										SUBSTRING(DB_NAME() + ' - ' + 'dbo.usp_mpDatabaseBackup' + ' - Backup User Databases (full)' + CASE WHEN @forSQLServerName <> @@SERVERNAME THEN ' - ' + REPLACE(@forSQLServerName, '\', '$') + ' ' ELSE ' - ' END + '[' + X.[database_name] + ']', 1, 128) AS [job_name],
										'Run'		AS [job_step_name],
										DB_NAME()	AS [job_database_name],
										'EXEC [dbo].[usp_mpDatabaseBackup] @sqlServerName = ''' + @forSQLServerName + N''', @dbName = ''' + X.[database_name] + N''', @backupLocation = DEFAULT, @flgActions = 1, @flgOptions = DEFAULT,	@retentionDays = DEFAULT, @executionLevel = DEFAULT, @debugMode = ' + CAST(@debugMode AS [varchar])
								FROM
									(
										SELECT [name] AS [database_name]
										FROM [dbo].[catalogDatabaseNames]
										WHERE	[project_id] = @projectID
												AND [instance_id] = @forInstanceID
												AND [active] = 1
												AND [name] NOT IN ('master', 'model', 'msdb', 'tempdb', 'distribution')														
									)X

						/*-------------------------------------------------------------------*/
						/* Weekly: System Databases (full) - only once a week on Saturday */
						IF @flgActions & 4096 = 4096 AND [dbo].[ufn_mpCheckTaskSchedulerForDate](@projectCode, 'dbo.usp_mpDatabaseBackup', 'System Databases (full)', GETDATE()) = 1
								INSERT INTO @jobExecutionQueue (  [instance_id], [project_id], [module], [descriptor]
																, [for_instance_id], [job_name], [job_step_name], [job_database_name]
																, [job_command])
								SELECT	@instanceID AS [instance_id], @projectID AS [project_id], @module AS [module], @codeDescriptor AS [descriptor],
										@forInstanceID AS [for_instance_id], 
										SUBSTRING(DB_NAME() + ' - ' + 'dbo.usp_mpDatabaseBackup' + ' - Backup System Databases (full)' + CASE WHEN @forSQLServerName <> @@SERVERNAME THEN ' - ' + REPLACE(@forSQLServerName, '\', '$') + ' ' ELSE ' - ' END + '[' + X.[database_name] + ']', 1, 128) AS [job_name],
										'Run'		AS [job_step_name],
										DB_NAME()	AS [job_database_name],
										'EXEC [dbo].[usp_mpDatabaseBackup] @sqlServerName = ''' + @forSQLServerName + N''', @dbName = ''' + X.[database_name] + N''', @backupLocation = DEFAULT, @flgActions = 1, @flgOptions = DEFAULT,	@retentionDays = DEFAULT, @executionLevel = DEFAULT, @debugMode = ' + CAST(@debugMode AS [varchar])
								FROM
									(
										SELECT [name] AS [database_name]
										FROM [dbo].[catalogDatabaseNames]
										WHERE	[project_id] = @projectID
												AND [instance_id] = @forInstanceID
												AND [active] = 1
												AND [name] IN ('master', 'model', 'msdb', 'distribution')														
									)X
					end

				------------------------------------------------------------------------------------------------------------------------------------------
				IF @codeDescriptor = 'dbo.usp_mpDatabaseBackup(Log)'
					begin
						/*-------------------------------------------------------------------*/
						/* Hourly: Backup User Databases Transaction Log */
						IF @flgActions & 8192 = 8192 AND [dbo].[ufn_mpCheckTaskSchedulerForDate](@projectCode, 'dbo.usp_mpDatabaseBackup', 'User Databases Transaction Log', GETDATE()) = 1
								INSERT INTO @jobExecutionQueue (  [instance_id], [project_id], [module], [descriptor]
																, [for_instance_id], [job_name], [job_step_name], [job_database_name]
																, [job_command])
								SELECT	@instanceID AS [instance_id], @projectID AS [project_id], @module AS [module], @codeDescriptor AS [descriptor],
										@forInstanceID AS [for_instance_id], 
										SUBSTRING(DB_NAME() + ' - ' + 'dbo.usp_mpDatabaseBackup' + ' - Backup User Databases (log)' + CASE WHEN @forSQLServerName <> @@SERVERNAME THEN ' - ' + REPLACE(@forSQLServerName, '\', '$') + ' ' ELSE ' - ' END + '[' + X.[database_name] + ']', 1, 128) AS [job_name],
										'Run'		AS [job_step_name],
										DB_NAME()	AS [job_database_name],
										'EXEC [dbo].[usp_mpDatabaseBackup] @sqlServerName = ''' + @forSQLServerName + N''', @dbName = ''' + X.[database_name] + N''', @backupLocation = DEFAULT, @flgActions = 4, @flgOptions = DEFAULT,	@retentionDays = DEFAULT, @executionLevel = DEFAULT, @debugMode = ' + CAST(@debugMode AS [varchar])
								FROM
									(
										SELECT [name] AS [database_name]
										FROM [dbo].[catalogDatabaseNames]
										WHERE	[project_id] = @projectID
												AND [instance_id] = @forInstanceID
												AND [active] = 1
												AND [name] NOT IN ('master', 'model', 'msdb', 'tempdb', 'distribution')	
									)X
						end
				------------------------------------------------------------------------------------------------------------------------------------------

				IF @recreateMode = 0
					UPDATE jeq
						SET   jeq.[execution_date] = NULL
							, jeq.[running_time_sec] = NULL
							, jeq.[log_message] = NULL
							, jeq.[status] = -1
							, jeq.[event_date_utc] = GETUTCDATE()
					FROM [dbo].[jobExecutionQueue] jeq
					INNER JOIN @jobExecutionQueue S ON		jeq.[instance_id] = S.[instance_id]
														AND jeq.[project_id] = S.[project_id]
														AND jeq.[module] = S.[module]
														AND jeq.[descriptor] = S.[descriptor]
														AND jeq.[for_instance_id] = S.[for_instance_id]
														AND jeq.[job_name] = S.[job_name]
														AND jeq.[job_step_name] = S.[job_step_name]
														AND jeq.[job_database_name] = S.[job_database_name]

				IF @recreateMode = 1 OR (@recreateMode = 0 AND @@ROWCOUNT=0)
					INSERT	INTO [dbo].[jobExecutionQueue](  [instance_id], [project_id], [module], [descriptor]
														   , [for_instance_id], [job_name], [job_step_name], [job_database_name]
														   , [job_command])
							SELECT	  S.[instance_id], S.[project_id], S.[module], S.[descriptor]
									, S.[for_instance_id], S.[job_name], S.[job_step_name], S.[job_database_name]
									, S.[job_command]
							FROM @jobExecutionQueue S
							LEFT JOIN [dbo].[jobExecutionQueue] jeq ON		jeq.[instance_id] = S.[instance_id]
																		AND jeq.[project_id] = S.[project_id]
																		AND jeq.[module] = S.[module]
																		AND jeq.[descriptor] = S.[descriptor]
																		AND jeq.[for_instance_id] = S.[for_instance_id]
																		AND jeq.[job_name] = S.[job_name]
																		AND jeq.[job_step_name] = S.[job_step_name]
																		AND jeq.[job_database_name] = S.[job_database_name]
							WHERE jeq.[job_name] IS NULL

				FETCH NEXT FROM crsCollectorDescriptor INTO @codeDescriptor
			end
		CLOSE crsCollectorDescriptor
		DEALLOCATE crsCollectorDescriptor
										

		FETCH NEXT FROM crsActiveInstances INTO @forInstanceID, @forSQLServerName
	end
CLOSE crsActiveInstances
DEALLOCATE crsActiveInstances


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
		@flgOptions					[int]		= 45185,--32768 + 8192 + 4096 + 128 + 1
		@DefragIndexThreshold		[smallint]	=     5,
		@RebuildIndexThreshold		[smallint]	=    30,
		@PageThreshold				[int]		=  1000,
		@RebuildIndexPageCountLimit	[int]		= 2147483647,	--16TB/no limit
		@StatsSamplePercent			[smallint]	=   100,
		@StatsAgeDays				[smallint]	=   365,
		@StatsChangePercent			[smallint]	=     1,
		@MaxDOP						[smallint]	=	  1,
		@MaxRunningTimeInMinutes	[smallint]	=     0,
		@skipObjectsList			[nvarchar](1024) = NULL,
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
--					   512  - update auto-created statistics
--					  1024	- get index statistics using DETAILED analysis (default is to use LIMITED)
--							  for heaps, will always use DETAILED in order to get page density and forwarded records information
--					  4096  - rebuild/reorganize indexes/tables using ONLINE=ON, if applicable (default)
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
--		@skipObjectsList			- comma separated list of the objects (tables, index name or stats name) to be excluded from maintenance.
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

--------------------------------------------------------------------------------------------------
/* AlwaysOn Availability Groups */
DECLARE @agName				[sysname],
		@agInstanceRoleDesc	[sysname],
		@agStopLimit		[int],
		@actionType			[sysname]

SET @agStopLimit = 0

IF @flgActions &  1 =  1	SET @actionType = 'reorganize index'
IF @flgActions &  2 =  2	SET @actionType = 'rebuilding index'
IF @flgActions &  4 =  4	SET @actionType = 'rebuilding index'
IF @flgActions &  8 =  8	SET @actionType = 'update statistics'
IF @flgActions & 16 = 16	SET @actionType = 'rebuilding heap'

IF @serverVersionNum >= 11
	EXEC @agStopLimit = [dbo].[usp_mpCheckAvailabilityGroupLimitations]	@sqlServerName		= @SQLServerName,
																		@dbName				= @DBName,
																		@actionName			= 'database maintenance',
																		@actionType			= @actionType,
																		@flgActions			= @flgActions,
																		@flgOptions			= @flgOptions OUTPUT,
																		@agName				= @agName OUTPUT,
																		@agInstanceRoleDesc = @agInstanceRoleDesc OUTPUT,
																		@executionLevel		= @executionLevel,
																		@debugMode			= @DebugMode

IF @agStopLimit <> 0
	RETURN 0

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
									AND ob.[type] IN (''U'', ''V'')' + 
									CASE WHEN @skipObjectsList IS NOT NULL  THEN N'	AND ob.[name] NOT IN (SELECT [value] FROM [' + DB_NAME() + N'].[dbo].[ufn_getTableFromStringList](''' + @skipObjectsList + N''', '',''))
																					AND (si.[name] NOT IN (SELECT [value] FROM [' + DB_NAME() + N'].[dbo].[ufn_getTableFromStringList](''' + @skipObjectsList + N''', '','')) OR si.[name] IS NULL)'  
																			ELSE N'' END

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

		DECLARE crsTableList CURSOR LOCAL FAST_FORWARD FOR 	SELECT	DISTINCT doil.[table_schema], doil.[table_name], doil.[avg_fragmentation_in_percent], doil.[page_count], doil.[page_density_deviation], doil.[forwarded_records_percentage]
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
															@MaxDOP				= @MaxDOP,
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
										AND ob.[type] IN (''U'', ''V'')' + 
										CASE WHEN @skipObjectsList IS NOT NULL	THEN N'	AND ob.[name] NOT IN (SELECT [value] FROM [' + DB_NAME() + N'].[dbo].[ufn_getTableFromStringList](''' + @skipObjectsList + N''', '','')) 
																						AND si.[name] NOT IN (SELECT [value] FROM [' + DB_NAME() + N'].[dbo].[ufn_getTableFromStringList](''' + @skipObjectsList + N''', '',''))' 
																				ELSE N'' END
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
										AND si.[reserved] <> 0
										AND ob.[xtype] IN (''U'', ''V'')'+
										CASE WHEN @skipObjectsList IS NOT NULL	THEN N'	AND ob.[name] NOT IN (SELECT [value] FROM [' + DB_NAME() + N'].[dbo].[ufn_getTableFromStringList](''' + @skipObjectsList + N''', '','')) 
																						AND si.[name] NOT IN (SELECT [value] FROM [' + DB_NAME() + N'].[dbo].[ufn_getTableFromStringList](''' + @skipObjectsList + N''', '',''))' 
																				ELSE N'' END

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
												, (ABS(sp.[modification_counter]) * 100. / CAST(sp.[rows] AS [float])) AS [percent_changes]
										FROM [' + @DBName + '].sys.stats ss
										INNER JOIN [' + @DBName + '].sys.objects ob	ON ob.[object_id] = ss.[object_id]
										INNER JOIN [' + @DBName + '].sys.schemas sc	ON sc.[schema_id] = ob.[schema_id]' + N'
										CROSS APPLY [' + @DBName + '].sys.dm_db_stats_properties(ss.object_id, ss.stats_id) AS sp
										WHERE	ob.[name] LIKE ''' + @TableName + '''
												AND sc.[name] LIKE ''' + @TableSchema + '''
												AND ob.[type] <> ''S''
												AND sp.[rows] > 0
												AND (    (    DATEDIFF(dd, sp.[last_updated], GETDATE()) >= ' + CAST(@StatsAgeDays AS [nvarchar](32)) + N' 
														  AND sp.[modification_counter] <> 0
														 )
													 OR  
														 ( 
															  DATEDIFF(dd, sp.[last_updated], GETDATE()) < ' + CAST(@StatsAgeDays AS [nvarchar](32)) + N' 
														  AND sp.[modification_counter] <> 0 
														  AND (ABS(sp.[modification_counter]) * 100. / CAST(sp.[rows] AS [float])) >= ' + CAST(@StatsChangePercent AS [nvarchar](32)) + N'
														 )
													)'+
												CASE WHEN @skipObjectsList IS NOT NULL	THEN N'	AND ob.[name] NOT IN (SELECT [value] FROM [' + DB_NAME() + N'].[dbo].[ufn_getTableFromStringList](''' + @skipObjectsList + N''', '','')) 
																								AND ss.[name] NOT IN (SELECT [value] FROM [' + DB_NAME() + N'].[dbo].[ufn_getTableFromStringList](''' + @skipObjectsList + N''', '',''))' 
																						ELSE N'' END
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
										INNER JOIN [' + @DBName + ']..sysindexes si ON si.[id] = ob.[object_id] AND si.[name] = ss.[name]' + N'
										WHERE	ob.[name] LIKE ''' + @TableName + '''
												AND sc.[name] LIKE ''' + @TableSchema + '''
												AND ob.[type] <> ''S''
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
												)' +
												CASE WHEN @skipObjectsList IS NOT NULL THEN N'	AND ob.[name] NOT IN (SELECT [value] FROM [' + DB_NAME() + N'].[dbo].[ufn_getTableFromStringList](''' + @skipObjectsList + N''', '','')) 
																								AND ss.[name] NOT IN (SELECT [value] FROM [' + DB_NAME() + N'].[dbo].[ufn_getTableFromStringList](''' + @skipObjectsList + N''', '',''))
																								AND si.[name] NOT IN (SELECT [value] FROM [' + DB_NAME() + N'].[dbo].[ufn_getTableFromStringList](''' + @skipObjectsList + N''', '',''))'
																					   ELSE N'' END
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
											)' + 
											CASE WHEN @skipObjectsList IS NOT NULL THEN N'	AND ob.[name] NOT IN (SELECT [value] FROM [' + DB_NAME() + N'].[dbo].[ufn_getTableFromStringList](''' + @skipObjectsList + N''', '','')) 
																							AND si.[name] NOT IN (SELECT [value] FROM [' + DB_NAME() + N'].[dbo].[ufn_getTableFromStringList](''' + @skipObjectsList + N''', '',''))'
																				   ELSE N'' END

		IF @SQLServerName<>@@SERVERNAME
			SET @queryToRun = N'SELECT x.* FROM OPENQUERY([' + @SQLServerName + N'], ''EXEC [' + @DBName + N']..sp_executesql N''''' + REPLACE(@queryToRun, '''', '''''''''') + ''''''')x'


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
		
		DECLARE crsTableList CURSOR LOCAL FAST_FORWARD FOR	SELECT	DISTINCT doil.[table_schema], doil.[table_name]
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

				DECLARE crsIndexesToDegfragment CURSOR LOCAL FAST_FORWARD FOR 	SELECT	DISTINCT doil.[index_name], doil.[index_type], doil.[avg_fragmentation_in_percent], doil.[page_count], doil.[object_id], doil.[index_id], doil.[page_density_deviation], doil.[fill_factor]
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
																		
		DECLARE crsTableList CURSOR LOCAL FAST_FORWARD FOR 	SELECT	DISTINCT doil.[table_schema], doil.[table_name]
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

		DECLARE crsTableList2 CURSOR LOCAL FAST_FORWARD FOR	SELECT [table_schema], [table_name], COUNT(*) AS [stats_count]
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
				DECLARE crsTableStatsList CURSOR LOCAL FAST_FORWARD FOR	SELECT	  [stats_name], [auto_created], [rows], [modification_counter], [last_updated], [percent_changes]
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
						SET @queryToRun = @queryToRun + N'IF OBJECT_ID(''[' + @CurrentTableSchema + '].[' + @CurrentTableName + ']'') IS NOT NULL UPDATE STATISTICS [' + @CurrentTableSchema + '].[' + @CurrentTableName + '](' + dbo.ufn_mpObjectQuoteName(@IndexName) + ') WITH '
								
						IF @StatsSamplePercent<100
							SET @queryToRun=@queryToRun + N'SAMPLE ' + CAST(@StatsSamplePercent AS [nvarchar]) + ' PERCENT'
						ELSE
							SET @queryToRun=@queryToRun + N'FULLSCAN'

						IF @DebugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0
						
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

		--128  - Create statistics on index columns only (default). Default behaviour is to not create statistics on all eligible columns
		IF @flgOptions & 128 = 128
			begin
				SET @queryToRun=N'Creating statistics for all tables / index columns only ...'
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

				SET @queryToRun = N''
				SET @queryToRun = @queryToRun + N'sp_createstats @indexonly = ''indexonly'''

				--256  - Create statistics using default sample scan. Default behaviour is to create statistics using fullscan mode
				IF @flgOptions & 256 = 256
					SET @queryToRun = @queryToRun + N', @fullscan = ''NO'''
				ELSE
					SET @queryToRun = @queryToRun + N', @fullscan = ''fullscan'''

				IF @DebugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

				SET @nestedExecutionLevel = @executionLevel + 1

				EXEC @errorCode = [dbo].[usp_sqlExecuteAndLog]	@sqlServerName	= @SQLServerName,
																@dbName			= @DBName,
																@objectName		= @objectName,
																@childObjectName= @childObjectName,
																@module			= 'dbo.usp_mpDatabaseOptimize',
																@eventName		= 'database maintenance - create statistics',
																@queryToRun  	= @queryToRun,
																@flgOptions		= @flgOptions,
																@executionLevel	= @nestedExecutionLevel,
																@debugMode		= @DebugMode
			end
	end
	

---------------------------------------------------------------------------------------------
IF object_id('tempdb..#CurrentIndexFragmentationStats') IS NOT NULL DROP TABLE #CurrentIndexFragmentationStats
IF object_id('tempdb..#databaseObjectsWithIndexList') IS NOT NULL 	DROP TABLE #databaseObjectsWithIndexList

RETURN @errorCode
GO



/*---------------------------------------------------------------------------------------------------------------------*/
/* patch SQL Agent Jobs																								   */
/*---------------------------------------------------------------------------------------------------------------------*/
RAISERROR('Patching SQL AGENT JOBS', 10, 1) WITH NOWAIT

RAISERROR('update job: Database Maintenance - User DBs - Parallel', 10, 1) WITH NOWAIT
GO
DECLARE   @queryToRun	[nvarchar](max)
		, @job_id		[uniqueidentifier]

SELECT @job_id = [job_id]
FROM msdb.dbo.sysjobs 
WHERE [name]  = (DB_NAME() + ' - Database Maintenance - User DBs - Parallel')

SET @queryToRun=N'EXEC [dbo].[usp_mpJobQueueCreate]	@projectCode		= DEFAULT,
													@module				= ''maintenance-plan'',
													@sqlServerNameFilter = @@SERVERNAME,
													@jobDescriptor		=''dbo.usp_mpDatabaseConsistencyCheck;dbo.usp_mpDatabaseOptimize;dbo.usp_mpDatabaseShrink'',
													@flgActions			= DEFAULT,
													@recreateMode		= DEFAULT,
													@debugMode			= DEFAULT'
IF @job_id IS NOT NULL
	EXEC msdb.dbo.sp_update_jobstep   @job_id = @job_id
									, @step_id = 1
									, @command = @queryToRun
GO


RAISERROR('update job: Database Backup - Log - Parallel', 10, 1) WITH NOWAIT
GO
DECLARE   @queryToRun	[nvarchar](max)
		, @job_id		[uniqueidentifier]

SELECT @job_id = [job_id]
FROM msdb.dbo.sysjobs 
WHERE [name] = (DB_NAME() + ' - Database Backup - Log - Parallel')

SET @queryToRun=N'EXEC [dbo].[usp_mpJobQueueCreate]	@projectCode		= DEFAULT,
													@module				= ''maintenance-plan'',
													@sqlServerNameFilter= @@SERVERNAME,
													@jobDescriptor		=''dbo.usp_mpDatabaseBackup(Log)'',
													@flgActions			= DEFAULT,
													@recreateMode		= DEFAULT,
													@debugMode			= DEFAULT'
IF @job_id IS NOT NULL
	EXEC msdb.dbo.sp_update_jobstep   @job_id = @job_id
									, @step_id = 1
									, @command = @queryToRun
GO


RAISERROR('update job: Database Backup - Full and Diff - Parallel', 10, 1) WITH NOWAIT
GO
DECLARE   @queryToRun	[nvarchar](max)
		, @job_id		[uniqueidentifier]

SELECT @job_id = [job_id]
FROM msdb.dbo.sysjobs 
WHERE [name]  = (DB_NAME() + ' - Database Backup - Full and Diff - Parallel')

SET @queryToRun=N'EXEC [dbo].[usp_mpJobQueueCreate]	@projectCode		= DEFAULT,
													@module				= ''maintenance-plan'',
													@sqlServerNameFilter= @@SERVERNAME,
													@jobDescriptor		=''dbo.usp_mpDatabaseBackup(Data)'',
													@flgActions			= DEFAULT,
													@recreateMode		= DEFAULT,
													@debugMode			= DEFAULT'

IF @job_id IS NOT NULL
	EXEC msdb.dbo.sp_update_jobstep   @job_id = @job_id
									, @step_id = 1
									, @command = @queryToRun
GO

/*---------------------------------------------------------------------------------------------------------------------*/
USE [dbaTDPMon]
GO
SELECT * FROM [dbo].[appConfigurations] WHERE [module] = 'common' AND [name] = 'Application Version'
GO

RAISERROR('* Done *', 10, 1) WITH NOWAIT

