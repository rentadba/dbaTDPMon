RAISERROR('Create procedure: [dbo].[usp_getUnmonitoredDatabases]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (
	    SELECT * 
	      FROM sysobjects 
	     WHERE id = OBJECT_ID(N'[dbo].[usp_getUnmonitoredDatabases]') 
	       AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[usp_getUnmonitoredDatabases]
GO

CREATE PROCEDURE [dbo].[usp_getUnmonitoredDatabases]
	@sqlServerNameFilter	[sysname]='%',
	@debugMode				[bit] = 0
AS
SET NOCOUNT ON;    

DECLARE @projectID		[smallint],
		@projectCode	[varchar](32),
		@instanceID		[smallint],
		@instanceName	[sysname],
		@queryToRun		[nvarchar](max),
		@queryParam		[nvarchar](128),
		@strMessage		[nvarchar](4000)

DECLARE @databasesNotMonitored TABLE 
	(
		[instance_name] [sysname],
		[database_name] [sysname]
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
DECLARE crsActiveInstances CURSOR LOCAL FAST_FORWARD FOR 	SELECT	MAX(cin.[instance_id]) AS [instance_id], cin.[instance_name]
															FROM	[dbo].[vw_catalogInstanceNames] cin
															WHERE 	cin.[instance_active]=1
																	AND cin.[instance_name] LIKE @sqlServerNameFilter
															GROUP BY cin.[instance_name]
															ORDER BY cin.[instance_name]
OPEN crsActiveInstances
FETCH NEXT FROM crsActiveInstances INTO @instanceID, @instanceName
WHILE @@FETCH_STATUS=0
	begin
		SET @strMessage='Analyzing server: ' + @instanceName
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 1, @messageTreelevel = 1, @stopExecution=0

		SET @queryToRun = N''
		SET @queryToRun = @queryToRun + N'SELECT [name], [database_id] 
										FROM sys.databases 										
										WHERE	[database_id]>4
												AND [source_database_id] IS NULL 
												AND [state_desc] <> ''OFFLINE'''
		SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@instanceName, @queryToRun)
		
		SET @queryToRun=N'WITH catalogInstances AS 
							(
								SELECT cin.[id] 
								FROM dbo.catalogInstanceNames AS cin
								INNER JOIN dbo.catalogProjects AS cp ON cp.[id] = cin.[project_id]
								WHERE cp.[active] = 1 
										AND cin.[active] = 1 
										AND cin.[name] = @instanceName
							)
						 SELECT @instanceName AS [instance_name]
								, x.[name] AS [database_name]
						 FROM (' + @queryToRun + ') x
						 LEFT JOIN dbo.catalogDatabaseNames cdn ON cdn.[database_id] = x.[database_id] 
																	AND cdn.[instance_id] IN (SELECT [id] FROM catalogInstances)
																	AND cdn.[name] = x.[name]
																	AND cdn.[active] = 1
						 WHERE cdn.[id] IS NULL'
		SET @queryParam = N'@instanceName [sysname]'
		IF @debugMode = 1 EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0

		BEGIN TRY
			INSERT	INTO @databasesNotMonitored([instance_name], [database_name])
					EXEC sp_executesql @queryToRun, @queryParam, @instanceName = @instanceName
		END TRY	
		BEGIN CATCH
			SET @strMessage = ERROR_MESSAGE()
			EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0

			INSERT	INTO [dbo].[logAnalysisMessages]([instance_id], [project_id], [event_date_utc], [descriptor], [message])
					SELECT  @instanceID
							, @projectID
							, GETUTCDATE()
							, 'dbo.usp_getUnmonitoredDatabases'
							, @strMessage
		END CATCH

		FETCH NEXT FROM crsActiveInstances INTO @instanceID, @instanceName
	end
CLOSE crsActiveInstances
DEALLOCATE crsActiveInstances

SELECT * FROM @databasesNotMonitored ORDER BY [instance_name], [database_name]
GO
