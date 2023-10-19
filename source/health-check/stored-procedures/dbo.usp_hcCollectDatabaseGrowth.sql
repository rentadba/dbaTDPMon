SET QUOTED_IDENTIFIER ON 
GO
SET ANSI_NULLS ON 
GO

RAISERROR('Create procedure: [dbo].[usp_hcCollectDatabaseGrowth]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (
	    SELECT * 
	      FROM sys.objects 
	     WHERE object_id = OBJECT_ID(N'[dbo].[usp_hcCollectDatabaseGrowth]') 
	       AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[usp_hcCollectDatabaseGrowth]
GO

CREATE PROCEDURE [dbo].[usp_hcCollectDatabaseGrowth]
		@projectCode			[varchar](32)=NULL,
		@sqlServerNameFilter	[sysname]='%',
		@databaseNameFilter		[sysname]='%',
		@debugMode				[bit]=0
AS
SET NOCOUNT ON;

------------------------------------------------------------------------------------------------------------------------------------------
DECLARE @projectID				[smallint],
		@sqlServerName			[sysname],
		@instanceID				[smallint],
		@isAzureSQLDatabase		[bit],
		@queryToRun				[nvarchar](max),
		@queryToRunTemplate		[nvarchar](max),
		@strMessage				[nvarchar](1024),
		@lastEventTime			[datetime]

IF object_id('#dbGrowthStats') IS NOT NULL DROP TABLE #dbGrowthStats
CREATE TABLE #dbGrowthStats
	(
		[database_name]		[nvarchar](128) NOT NULL,
		[logical_name]		[nvarchar](255) NOT NULL,
		[current_size_kb]	[bigint] NOT NULL,
		[file_type]			[nvarchar](10) NOT NULL,
		[growth_type]		[nvarchar](50) NOT NULL,
		[growth_kb]			[int] NOT NULL,
		[duration]			[int] NOT NULL,
		[start_time]		[datetime] NOT NULL,
		[end_time]			[datetime] NOT NULL,
		[session_id]		[smallint] NOT NULL,
		[login_name]		[sysname] NULL,
		[host_name]			[sysname] NULL,
		[application_name]	[sysname] NULL,
		[client_process_id]	[int] NULL
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

DECLARE @folderSeparator nvarchar(16)
SET @folderSeparator = [dbo].[ufn_formatPlatformSpecificPath] (@@SERVERNAME, '\')

-------------------------------------------------------------------------------------------------------------------------
SET @queryToRunTemplate = N''
SET @queryToRunTemplate = @queryToRunTemplate + N'
DECLARE   @curr_tracefilename [varchar](500)
		, @indx [int]
		, @base_tracefilename [varchar](500);

SELECT @curr_tracefilename = path FROM sys.traces WHERE is_default = 1;
SET @curr_tracefilename = REVERSE(@curr_tracefilename);
SELECT @indx = PATINDEX(''%{folderSeparator}%'', @curr_tracefilename) ;
SET @curr_tracefilename = REVERSE(@curr_tracefilename) ;
SET @base_tracefilename = LEFT( @curr_tracefilename,LEN(@curr_tracefilename) - @indx) + ''{folderSeparator}log.trc'';

WITH AutoGrow_CTE (DatabaseID, FileName, Growth, Duration, StartTime, EndTime, SPID, LoginName, HostName, ApplicationName, ClientProcessID)
AS
(
	SELECT DatabaseID, FileName, SUM(IntegerData*8) AS Growth, Duration, StartTime, EndTime, SPID, LoginName, HostName, ApplicationName, ClientProcessID
	FROM ::fn_trace_gettable(@base_tracefilename, default)
	WHERE	EventClass >= 92 
		AND EventClass <= 95
		AND StartTime > @StartTime
	GROUP BY DatabaseID, FileName, IntegerData, Duration, StartTime, EndTime, SPID, LoginName, HostName, ApplicationName, ClientProcessID
)
SELECT	  DB_NAME(database_id) AS DatabaseName
		, mf.name AS LogicalName
		, CAST(mf.size as bigint) *8 AS CurrentSize_KB
		, mf.type_desc AS ''File_Type''
		, CASE WHEN is_percent_growth = 1 THEN ''Percentage'' ELSE ''Pages'' END AS ''Growth_Type''
		, ag.Growth AS Growth_KB
		, Duration/1000 AS Duration_ms
		, ag.StartTime
		, ag.EndTime
		, ag.SPID, ag.LoginName, ag.HostName, ag.ApplicationName, ag.ClientProcessID
FROM sys.master_files mf
INNER JOIN AutoGrow_CTE ag ON mf.database_id = ag.DatabaseID AND mf.name = ag.FileName
WHERE ag.Growth > 0 --Only where growth occurred
GROUP BY database_id, mf.name, mf.size, ag.Growth, ag.Duration, ag.StartTime, ag.EndTime, is_percent_growth, mf.growth, mf.type_desc
		, ag.SPID, ag.LoginName, ag.HostName, ag.ApplicationName, ag.ClientProcessID
ORDER BY DatabaseName, LogicalName, ag.StartTime'

-------------------------------------------------------------------------------------------------------------------------
SET @strMessage='Step 1: Get Database(s) Growth details....'
EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0
		
DECLARE crsActiveInstances CURSOR LOCAL FAST_FORWARD FOR 	SELECT	cin.[instance_id], cin.[instance_name], 
																	CASE WHEN cin.[engine] IN (5, 6) THEN 1 ELSE 0 END AS [isAzureSQLDatabase]
															FROM	[dbo].[vw_catalogInstanceNames] cin
															WHERE 	cin.[project_id] = @projectID
																	AND cin.[instance_active]=1
																	AND cin.[instance_name] LIKE @sqlServerNameFilter
															ORDER BY cin.[instance_name]
OPEN crsActiveInstances
FETCH NEXT FROM crsActiveInstances INTO @instanceID, @sqlServerName, @isAzureSQLDatabase
WHILE @@FETCH_STATUS=0
	begin
		SET @strMessage='Analyzing server: ' + @sqlServerName
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 1, @messageTreelevel = 1, @stopExecution=0

		SELECT @lastEventTime = MAX([start_time])
		FROM [health-check].[statsDatabaseGrowth]
		WHERE [project_id] = @projectID
			AND [instance_id] = @instanceID

		SET @queryToRun = @queryToRunTemplate
		SET @queryToRun = REPLACE(@queryToRun, '@StartTime', '''' + CONVERT([nvarchar](24), ISNULL(@lastEventTime, '1900-01-01'), 121) + '''');
		SET @queryToRun = REPLACE(@queryToRun, '{folderSeparator}', @folderSeparator)
		
		IF @sqlServerName <> @@SERVERNAME
			SET @queryToRun = 'SELECT * FROM OPENQUERY([' + @sqlServerName + N'], ''' +  REPLACE(@queryToRun, '''', '''''') + ''')x' 		

		IF @debugMode = 1 EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0

		TRUNCATE TABLE #dbGrowthStats;
		INSERT	INTO #dbGrowthStats ([database_name], [logical_name], [current_size_kb], [file_type], [growth_type], [growth_kb], [duration], [start_time], [end_time], [session_id], [login_name], [host_name], [application_name], [client_process_id])
				EXEC sp_executesql @queryToRun

		INSERT	INTO [health-check].[statsDatabaseGrowth] ([project_id], [instance_id], [database_name], [logical_name], [current_size_kb], [file_type], [growth_type], [growth_kb], [duration], [start_time], [end_time], [session_id], [login_name], [host_name], [application_name], [client_process_id])
				SELECT    @projectID, @instanceID
						, [database_name], [logical_name], [current_size_kb], [file_type], [growth_type], [growth_kb], [duration], [start_time], [end_time], [session_id], [login_name], [host_name], [application_name], [client_process_id]
				FROM #dbGrowthStats

		FETCH NEXT FROM crsActiveInstances INTO @instanceID, @sqlServerName, @isAzureSQLDatabase
	end
CLOSE crsActiveInstances
DEALLOCATE crsActiveInstances
GO

