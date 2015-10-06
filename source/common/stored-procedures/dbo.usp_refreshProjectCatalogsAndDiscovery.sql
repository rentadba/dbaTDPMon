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
