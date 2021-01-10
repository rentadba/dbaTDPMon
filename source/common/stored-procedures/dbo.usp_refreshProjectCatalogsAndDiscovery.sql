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
		@projectCode				[varchar](32),
		@runDiscovery				[bit] = 0,	/* using sqlcmd -L*/
		@enableXPCMDSHELL			[bit] = 1,
		@addNewDatabasesToProject	[bit] = 1,
		@debugMode					[bit] = 0
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
		, @optionXPValue			[int]

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
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @errMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 0, @stopExecution=1
	end

-----------------------------------------------------------------------------------------------------
SET @errMessage=N'Step 1: Delete existing information....'
EXEC [dbo].[usp_logPrintMessage] @customMessage = @errMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0


DELETE lam 
FROM [dbo].[logAnalysisMessages] lam 
WHERE lam.[project_id] = @projectID

IF @runDiscovery=1
	begin		
		/*-------------------------------------------------------------------------------------------------------------------------------*/
		/* check if xp_cmdshell is enabled or should be enabled																			 */
		SET @optionXPValue = 0

		/* enable xp_cmdshell configuration option */
		EXEC [dbo].[usp_changeServerOption_xp_cmdshell]   @serverToRun	 = @sqlServerName
														, @flgAction	 = 1			-- 1=enable | 0=disable
														, @optionXPValue = @optionXPValue OUTPUT
														, @debugMode	 = @debugMode

		IF @optionXPValue = 0
			begin
				RETURN 1
			end		

		/*-------------------------------------------------------------------------------------------------------------------------------*/
		/* perform discovery																											 */
		/*-------------------------------------------------------------------------------------------------------------------------------*/
		SET @errMessage = 'Performing SQL Server instance discovery...'
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @errMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0


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
		EXEC [dbo].[usp_changeServerOption_xp_cmdshell]   @serverToRun	 = @sqlServerName
														, @flgAction	 = 0			-- 1=enable | 0=disable
														, @optionXPValue = @optionXPValue OUTPUT
														, @debugMode	 = @debugMode


		/*-------------------------------------------------------------------------------------------------------------------------------*/
		/* catalog discovered servers																									 */
		/*-------------------------------------------------------------------------------------------------------------------------------*/
		DECLARE crsDiscoveredServer CURSOR LOCAL FAST_FORWARD FOR	SELECT xp.[instance_name], ss.[server_id]
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
																		)cat ON	cat.[instance_name] = xp.[instance_name] COLLATE DATABASE_DEFAULT
																				OR cat.[machine_name] = xp.[instance_name] COLLATE DATABASE_DEFAULT
																				OR cat.[machine_name] = xp.[machine_name] COLLATE DATABASE_DEFAULT
																	LEFT  JOIN sys.servers	ss	ON	ss.[name] = xp.[instance_name] COLLATE DATABASE_DEFAULT
																	WHERE cat.[instance_name] IS NULL AND cat.[machine_name] IS NULL
		OPEN crsDiscoveredServer
		FETCH NEXT FROM crsDiscoveredServer INTO @sqlServerName, @existingServerID
		WHILE @@FETCH_STATUS=0
			begin
				SET @errMessage = 'New SQL Server Instance found: [' + @sqlServerName + ']'
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @errMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 1, @messageTreelevel = 1, @stopExecution=0
		
				IF @existingServerID IS NULL
					begin
						/* create a linked server for the instance found */
						EXEC [dbo].[usp_addLinkedSQLServer] @ServerName = @sqlServerName
					end
					
				/* catalog the instance */
				EXEC @instanceID = [dbo].[usp_refreshMachineCatalogs] 	@projectCode			  = @projectCode,
																		@sqlServerName			  = @sqlServerName,
																		@addNewDatabasesToProject = @addNewDatabasesToProject,
																		@debugMode				  = @debugMode


				INSERT	INTO [dbo].[logAnalysisMessages]([instance_id], [project_id], [event_date_utc], [descriptor], [message])
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
DECLARE crsDiscoveredServer CURSOR LOCAL FAST_FORWARD FOR	SELECT cin.[name], ss.[server_id]
															FROM [dbo].[catalogInstanceNames] cin 
															INNER JOIN [dbo].[catalogProjects]		cp	ON	cp.[id] = cin.[project_id] 
															INNER JOIN [dbo].[catalogMachineNames]  cmn ON	cmn.[id] = cin.[machine_id] 
																											AND cmn.[project_id] = cin.[project_id]
															LEFT  JOIN #xp_cmdshell					xp  ON	cin.[name] = xp.[output] COLLATE DATABASE_DEFAULT
																											OR cmn.[name] = xp.[output] COLLATE DATABASE_DEFAULT
															LEFT  JOIN sys.servers					ss	ON	ss.[name] = cin.[name] COLLATE DATABASE_DEFAULT
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
		EXEC [dbo].[usp_refreshMachineCatalogs] 	@projectCode			  = @projectCode,
													@sqlServerName			  = @sqlServerName,
													@addNewDatabasesToProject = @addNewDatabasesToProject,
													@debugMode				  = @debugMode
												
		FETCH NEXT FROM crsDiscoveredServer INTO @sqlServerName, @existingServerID
	end
CLOSE crsDiscoveredServer
DEALLOCATE crsDiscoveredServer
/*-------------------------------------------------------------------------------------------------------------------------------*/
GO
