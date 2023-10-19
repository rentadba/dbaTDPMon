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
		@sqlServerNameFilter	[sysname]='%',
		@flgActions				[int]			= 63,		/*	1 - Instance Availability 
																2 - Databases status
																4 - SQL Server Agent Job status
																8 - Disk Space information
															   16 - Errorlog messages
															   32 - OS Event messages
															*/
		@flgOptions				[bigint]		= 1876951039,/*	 1 - Instances - Offline
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
														   2097152 - NOT USED
														   4194304 - Databases with Fixed File(s) Size - Issues Detected													
														   8388608 - Databases with (Page Verify not CHECKSUM) or (Page Verify is NONE)
														  16777216 - Frequently Fragmented Indexes (consider lowering the fill-factor)
														  33554432 - SQL Server Agent Jobs - Long Running SQL Agent Jobs
														  67108864 - OS Event messages - Permission errors
														 134217728 - OS Event messages - Issues Detected
														 268435456 - do not consider @projectCode when filtering instance and database information
														 536870912 - Failed Login Attempts - Issues Detected
														1073741824 - Database Growth Information - Issues Detected
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
	
DECLARE   @databaseName								[sysname]
		, @reportOptionDatabaseAdmittedState		[sysname]
		, @reportOptionDatabaseMaxSizeMaster		[int]
		, @reportOptionDatabaseMaxSizeMSDB			[int]
		, @reportOptionLogMaxSize					[int]
		, @reportOptionLogVsDataPercent				[numeric](6,2)
		, @reportOptionDataSpaceMinPercent			[numeric](6,2)
		, @reportOptionLogSpaceMaxPercent			[numeric](6,2)
		, @reportOptionDBMinSizeForAnalysis 		[int]
		, @reportOptionJobFailuresInLastHours		[int]
		, @reportOptionUserDBCCCHECKDBAgeDays		[int]
		, @reportOptionSystemDBCCCHECKDBAgeDays		[int]
		, @reportOptionUserDatabaseBACKUPAgeDays	[int]
		, @reportOptionSystemDatabaseBACKUPAgeDays	[int]
		, @reportOptionFreeDiskMinPercent 			[numeric](6,2)
		, @reportOptionFreeDiskMinSpaceMB			[int]
		, @reportOptionFailedLoginAttemptsLimit		[int]
		, @reportOptionErrorlogMessageLastHours		[int]
		, @reportOptionErrorlogMessageLimit			[int]
		, @reportOptionMaxJobRunningTimeInHours		[int]
		, @reportOptionOSEventMessageLimit			[int]
		, @reportOptionOSEventMessageLastHours		[int]
		, @reportOptionGetProjectDBSize				[bit]
		, @reportOptionMinSpaceToReclaim			[int]
		, @reportOptionSkipDatabaseSnapshots		[bit]
		, @reportOptionGetBackupSizeLastDays		[smallint]
		, @reportOptionGetDBGrowthLastDays			[smallint]
		, @reportOptionDBGrowthMinPercentForAnalysis[int]
		, @reportOptionDBGrowthMinSizeMBForAnalysis	[int]
		, @configOSEventMessageLastHours			[int]
		, @configOSEventGetInformationEvent			[bit]
		, @configOSEventGetWarningsEvent			[bit]
		, @configOSEventsTimeOutSeconds				[int]

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
		, @diskTotalSizeGB						[numeric](18,3)
		, @diskAvailableSpaceGB					[numeric](18,3)
		, @diskPercentAvailable					[numeric](6,2)
		, @dateTimeLowerLimit					[datetime]
		, @dateTimeLowerLimitUTC				[datetime]
		, @dbCount								[int]

		, @messageCount							[int]
		, @issuesDetectedCount					[int]

DECLARE @eventMessageData						[varchar](8000)

/*-------------------------------------------------------------------------------------------------------------------------------*/
-- { sql_statement | statement_block }
BEGIN TRY
	SET @reportBuildStartTime = GETUTCDATE()
	SET @ReturnValue=1
	
	-----------------------------------------------------------------------------------------------------
	--get default projectCode
	IF @projectCode IS NULL
		SET @projectCode = [dbo].[ufn_getProjectCode](NULL, NULL)

	SELECT    @projectID = [id]
			, @projectName = [name]
	FROM [dbo].[catalogProjects]
	WHERE [code] = @projectCode 

	IF @projectID IS NULL
		begin
			SET @ErrMessage=N'The value specifief for Project Code is not valid.'
			EXEC [dbo].[usp_logPrintMessage] @customMessage = @ErrMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=1
		end
			
	-----------------------------------------------------------------------------------------------------
	SET @ErrMessage='Building Daily Health Check Report for: [' + @projectCode + ']'
	EXEC [dbo].[usp_logPrintMessage] @customMessage = @ErrMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0


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
	SELECT	@reportOptionDatabaseAdmittedState = [value]
	FROM	[report].[htmlOptions]
	WHERE	[name] = N'Database online admitted state'
			AND [module] = 'health-check'

	SET @reportOptionDatabaseAdmittedState = ISNULL(@reportOptionDatabaseAdmittedState, 'ONLINE, READ ONLY')
			
	-----------------------------------------------------------------------------------------------------
	BEGIN TRY
		SELECT	@reportOptionDatabaseMaxSizeMaster = [value]
		FROM	[report].[htmlOptions]
		WHERE	[name] = N'Database max size (mb) - master'
				AND [module] = 'health-check'
	END TRY
	BEGIN CATCH
		SET @reportOptionDatabaseMaxSizeMaster = 0
	END CATCH
	SET @reportOptionDatabaseMaxSizeMaster = ISNULL(@reportOptionDatabaseMaxSizeMaster, 0)

	-----------------------------------------------------------------------------------------------------
	BEGIN TRY
		SELECT	@reportOptionDatabaseMaxSizeMSDB = [value]
		FROM	[report].[htmlOptions]
		WHERE	[name] = N'Database max size (mb) - msdb'
				AND [module] = 'health-check'
	END TRY
	BEGIN CATCH
		SET @reportOptionDatabaseMaxSizeMSDB = 0
	END CATCH
	SET @reportOptionDatabaseMaxSizeMSDB = ISNULL(@reportOptionDatabaseMaxSizeMSDB, 0)
			
	-----------------------------------------------------------------------------------------------------
	BEGIN TRY
		SELECT	@reportOptionLogMaxSize = [value]
		FROM	[report].[htmlOptions]
		WHERE	[name] = N'Database Max Log Size (mb)'
				AND [module] = 'health-check'
	END TRY
	BEGIN CATCH
		SET @reportOptionLogMaxSize = 32768
	END CATCH
	SET @reportOptionLogMaxSize = ISNULL(@reportOptionLogMaxSize, 32768)
			
	-----------------------------------------------------------------------------------------------------
	BEGIN TRY
		SELECT	@reportOptionDataSpaceMinPercent = [value]
		FROM	[report].[htmlOptions]
		WHERE	[name] = N'Database Min Data Usage (percent)'
				AND [module] = 'health-check'
	END TRY
	BEGIN CATCH
		SET @reportOptionDataSpaceMinPercent = 50
	END CATCH
	SET @reportOptionDataSpaceMinPercent = ISNULL(@reportOptionDataSpaceMinPercent, 50)

	-----------------------------------------------------------------------------------------------------
	BEGIN TRY
		SELECT	@reportOptionLogSpaceMaxPercent = [value]
		FROM	[report].[htmlOptions]
		WHERE	[name] = N'Database Max Log Usage (percent)'
				AND [module] = 'health-check'
	END TRY
	BEGIN CATCH
		SET @reportOptionLogSpaceMaxPercent = 90
	END CATCH
	SET @reportOptionLogSpaceMaxPercent = ISNULL(@reportOptionLogSpaceMaxPercent, 90)

	-----------------------------------------------------------------------------------------------------
	BEGIN TRY
		SELECT	@reportOptionDBMinSizeForAnalysis  = [value]
		FROM	[report].[htmlOptions]
		WHERE	[name] = N'Database Min Size for Analysis (mb)'
				AND [module] = 'health-check'
	END TRY
	BEGIN CATCH
		SET @reportOptionDBMinSizeForAnalysis  = 512
	END CATCH
	SET @reportOptionDBMinSizeForAnalysis  = ISNULL(@reportOptionDBMinSizeForAnalysis , 512)

	-----------------------------------------------------------------------------------------------------			
	BEGIN TRY
		SELECT	@reportOptionLogVsDataPercent = [value]
		FROM	[report].[htmlOptions]
		WHERE	[name] = N'Database Log vs. Data Size (percent)'
				AND [module] = 'health-check'
	END TRY
	BEGIN CATCH
		SET @reportOptionLogVsDataPercent = 50
	END CATCH
	SET @reportOptionLogVsDataPercent = ISNULL(@reportOptionLogVsDataPercent, 50)
							
	-----------------------------------------------------------------------------------------------------			
	BEGIN TRY
		SELECT	@reportOptionMinSpaceToReclaim = [value]
		FROM	[report].[htmlOptions]
		WHERE	[name] = N'Minimum Disk space to reclaim (mb)'
				AND [module] = 'health-check'
	END TRY
	BEGIN CATCH
		SET @reportOptionMinSpaceToReclaim = 10240
	END CATCH
	SET @reportOptionMinSpaceToReclaim = ISNULL(@reportOptionMinSpaceToReclaim, 10240)
							
	-----------------------------------------------------------------------------------------------------
	BEGIN TRY
		SELECT	@reportOptionJobFailuresInLastHours = [value]
		FROM	[report].[htmlOptions]
		WHERE	[name] = N'SQL Agent Job - Failures in last hours'
				AND [module] = 'health-check'
	END TRY
	BEGIN CATCH
		SET @reportOptionJobFailuresInLastHours = 24
	END CATCH
	SET @reportOptionJobFailuresInLastHours = ISNULL(@reportOptionJobFailuresInLastHours, 24)

	-----------------------------------------------------------------------------------------------------
	BEGIN TRY
		SELECT	@reportOptionUserDatabaseBACKUPAgeDays = [value]
		FROM	[report].[htmlOptions]
		WHERE	[name] = N'User Database BACKUP Age (days)'
				AND [module] = 'health-check'
	END TRY
	BEGIN CATCH
		SET @reportOptionUserDatabaseBACKUPAgeDays = 2
	END CATCH
	SET @reportOptionUserDatabaseBACKUPAgeDays = ISNULL(@reportOptionUserDatabaseBACKUPAgeDays, 2)

	-----------------------------------------------------------------------------------------------------
	BEGIN TRY
		SELECT	@reportOptionSystemDatabaseBACKUPAgeDays = [value]
		FROM	[report].[htmlOptions]
		WHERE	[name] = N'System Database BACKUP Age (days)'
				AND [module] = 'health-check'
	END TRY
	BEGIN CATCH
		SET @reportOptionSystemDatabaseBACKUPAgeDays = 14
	END CATCH
	SET @reportOptionSystemDatabaseBACKUPAgeDays = ISNULL(@reportOptionSystemDatabaseBACKUPAgeDays, 14)

	-----------------------------------------------------------------------------------------------------
	BEGIN TRY
		SELECT	@reportOptionUserDBCCCHECKDBAgeDays = [value]
		FROM	[report].[htmlOptions]
		WHERE	[name] = N'User Database DBCC CHECKDB Age (days)'
				AND [module] = 'health-check'
	END TRY
	BEGIN CATCH
		SET @reportOptionUserDBCCCHECKDBAgeDays = 30
	END CATCH
	SET @reportOptionUserDBCCCHECKDBAgeDays = ISNULL(@reportOptionUserDBCCCHECKDBAgeDays, 30)

	-----------------------------------------------------------------------------------------------------
	BEGIN TRY
		SELECT	@reportOptionSystemDBCCCHECKDBAgeDays = [value]
		FROM	[report].[htmlOptions]
		WHERE	[name] = N'System Database DBCC CHECKDB Age (days)'
				AND [module] = 'health-check'
	END TRY
	BEGIN CATCH
		SET @reportOptionSystemDBCCCHECKDBAgeDays = 90
	END CATCH
	SET @reportOptionSystemDBCCCHECKDBAgeDays = ISNULL(@reportOptionSystemDBCCCHECKDBAgeDays, 90)

	-----------------------------------------------------------------------------------------------------
	BEGIN TRY
		SELECT	@reportOptionFreeDiskMinPercent  = [value]
		FROM	[report].[htmlOptions]
		WHERE	[name] = N'Free Disk Space Min Percent (percent)'
				AND [module] = 'health-check'
	END TRY
	BEGIN CATCH
		SET @reportOptionFreeDiskMinPercent  = 10
	END CATCH
	SET @reportOptionFreeDiskMinPercent  = ISNULL(@reportOptionFreeDiskMinPercent , 10)

	-----------------------------------------------------------------------------------------------------
	BEGIN TRY
		SELECT	@reportOptionFreeDiskMinSpaceMB = [value]
		FROM	[report].[htmlOptions]
		WHERE	[name] = N'Free Disk Space Min Space (mb)'
				AND [module] = 'health-check'
	END TRY
	BEGIN CATCH
		SET @reportOptionFreeDiskMinSpaceMB = 3000
	END CATCH
	SET @reportOptionFreeDiskMinSpaceMB = ISNULL(@reportOptionFreeDiskMinSpaceMB, 3000)

	-----------------------------------------------------------------------------------------------------
	BEGIN TRY
		SELECT	@reportOptionFailedLoginAttemptsLimit = [value]
		FROM	[report].[htmlOptions]
		WHERE	[name] = N'Minimum Failed Login Attempts'
				AND [module] = 'health-check'
	END TRY
	BEGIN CATCH
		SET @reportOptionFailedLoginAttemptsLimit = 50
	END CATCH
	SET @reportOptionFailedLoginAttemptsLimit = ISNULL(@reportOptionFailedLoginAttemptsLimit, 50)

	-----------------------------------------------------------------------------------------------------
	BEGIN TRY
		SELECT	@reportOptionErrorlogMessageLastHours = [value]
		FROM	[report].[htmlOptions]
		WHERE	[name] = N'Errorlog Messages in last hours'
				AND [module] = 'health-check'
	END TRY
	BEGIN CATCH
		SET @reportOptionErrorlogMessageLastHours = 24
	END CATCH
	SET @reportOptionErrorlogMessageLastHours = ISNULL(@reportOptionErrorlogMessageLastHours, 24)

	-----------------------------------------------------------------------------------------------------
	BEGIN TRY
		SELECT	@reportOptionErrorlogMessageLimit = [value]
		FROM	[report].[htmlOptions]
		WHERE	[name] = N'Errorlog Messages Limit to Max'
				AND [module] = 'health-check'
	END TRY
	BEGIN CATCH
		SET @reportOptionErrorlogMessageLimit = 1000
	END CATCH
	SET @reportOptionErrorlogMessageLimit = ISNULL(@reportOptionErrorlogMessageLimit, 1000)

	IF @reportOptionErrorlogMessageLimit= 0 SET @reportOptionErrorlogMessageLimit=2147483647

	-----------------------------------------------------------------------------------------------------
	BEGIN TRY
		SELECT	@reportOptionMaxJobRunningTimeInHours = [value]
		FROM	[report].[htmlOptions]
		WHERE	[name] = N'SQL Agent Job - Maximum Running Time (hours)'
				AND [module] = 'health-check'
	END TRY
	BEGIN CATCH
		SET @reportOptionMaxJobRunningTimeInHours = 3
	END CATCH
	SET @reportOptionMaxJobRunningTimeInHours = ISNULL(@reportOptionMaxJobRunningTimeInHours, 3)

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
		SELECT	@reportOptionOSEventMessageLimit = [value]
		FROM	[report].[htmlOptions]
		WHERE	[name] = N'OS Event Messages Limit to Max'
				AND [module] = 'health-check'
	END TRY
	BEGIN CATCH
		SET @reportOptionOSEventMessageLimit = 1000
	END CATCH
	SET @reportOptionOSEventMessageLimit = ISNULL(@reportOptionOSEventMessageLimit, 1000)

	IF @reportOptionOSEventMessageLimit= 0 SET @reportOptionOSEventMessageLimit=2147483647
		
	-----------------------------------------------------------------------------------------------------
	BEGIN TRY
		SELECT	@reportOptionOSEventMessageLastHours = [value]
		FROM	[report].[htmlOptions]
		WHERE	[name] = N'OS Event Messages in last hours'
				AND [module] = 'health-check'
	END TRY
	BEGIN CATCH
		SET @reportOptionOSEventMessageLastHours = 24
	END CATCH
	SET @reportOptionOSEventMessageLastHours = ISNULL(@reportOptionOSEventMessageLastHours, 24)

	-----------------------------------------------------------------------------------------------------
	BEGIN TRY
		SELECT	@reportOptionGetProjectDBSize = CASE WHEN [value]='true' THEN 1 ELSE 0 END
		FROM	[report].[htmlOptions]
		WHERE	[name] = N'Online Instance Get Databases Size per Project'
				AND [module] = 'health-check'
	END TRY
	BEGIN CATCH
		SET @reportOptionGetProjectDBSize = 0
	END CATCH
	SET @reportOptionGetProjectDBSize = ISNULL(@reportOptionGetProjectDBSize, 0)


	-----------------------------------------------------------------------------------------------------
	BEGIN TRY
		SELECT	@reportOptionSkipDatabaseSnapshots = CASE WHEN [value]='true' THEN 1 ELSE 0 END
		FROM	[report].[htmlOptions]
		WHERE	[name] = N'Exclude Database Snapshots for Backup/DBCC checks'
				AND [module] = 'health-check'
	END TRY
	BEGIN CATCH
		SET @reportOptionSkipDatabaseSnapshots = 1
	END CATCH
	SET @reportOptionSkipDatabaseSnapshots = ISNULL(@reportOptionSkipDatabaseSnapshots, 1)

	-----------------------------------------------------------------------------------------------------
	BEGIN TRY
		SELECT	@reportOptionGetBackupSizeLastDays = [value]
		FROM	[report].[htmlOptions]
		WHERE	[name] = N'Analyze backup size (GB) in the last days'
				AND [module] = 'health-check'
	END TRY
	BEGIN CATCH
		SET @reportOptionGetBackupSizeLastDays = 7
	END CATCH
	SET @reportOptionGetBackupSizeLastDays = ISNULL(@reportOptionGetBackupSizeLastDays, 7)
	
	-----------------------------------------------------------------------------------------------------
	BEGIN TRY
		SELECT	@reportOptionGetDBGrowthLastDays = [value]
		FROM	[report].[htmlOptions]
		WHERE	[name] = N'Analyze database(s) growth in the last days'
				AND [module] = 'health-check'
	END TRY
	BEGIN CATCH
		SET @reportOptionGetDBGrowthLastDays = 30
	END CATCH
	SET @reportOptionGetDBGrowthLastDays = ISNULL(@reportOptionGetDBGrowthLastDays, 30)
	
	-----------------------------------------------------------------------------------------------------
	BEGIN TRY
		SELECT	@reportOptionDBGrowthMinPercentForAnalysis = [value]
		FROM	[report].[htmlOptions]
		WHERE	[name] = N'Minimum database(s) growth percent'
				AND [module] = 'health-check'
	END TRY
	BEGIN CATCH
		SET @reportOptionDBGrowthMinPercentForAnalysis = 10
	END CATCH
	SET @reportOptionDBGrowthMinPercentForAnalysis = ISNULL(@reportOptionDBGrowthMinPercentForAnalysis, 10)
	
	-----------------------------------------------------------------------------------------------------
	BEGIN TRY
		SELECT	@reportOptionDBGrowthMinSizeMBForAnalysis = [value]
		FROM	[report].[htmlOptions]
		WHERE	[name] = N'Minimum database(s) growth size (mb)'
				AND [module] = 'health-check'
	END TRY
	BEGIN CATCH
		SET @reportOptionDBGrowthMinSizeMBForAnalysis = 32768
	END CATCH
	SET @reportOptionDBGrowthMinSizeMBForAnalysis = ISNULL(@reportOptionDBGrowthMinSizeMBForAnalysis, 32768)

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
		font-size:11px; 
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
	SELECT @HTMLReportArea = [value] FROM [dbo].[appConfigurations] WHERE [name]='Application Version' AND [module] = 'common'

	SET @ErrMessage ='Build Report: Header'
	EXEC [dbo].[usp_logPrintMessage] @customMessage = @ErrMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0

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
		<TD WIDTH="400px" ALIGN=LEFT VALIGN="TOP">
			<TABLE CELLSPACING=0 CELLPADDING="3px" border=0 width="400px">
			<TR VALIGN="TOP">
				<TD WIDTH="400px" VALIGN="TOP">
					<TABLE CELLSPACING=0 CELLPADDING="3px" border=0> 
						<TR>
							<TD ALIGN=RIGHT WIDTH="60px"><P class="title3-style">Project:</P></TD>
							<TD ALIGN=LEFT  WIDTH="340px"><P class="title-style">' +  @projectName + N'</P></TD>
						</TR>
						<TR>
							<TD ALIGN=RIGHT WIDTH="60px"><P class="title3-style">@</P></TD>
							<TD ALIGN=LEFT  WIDTH="340px"><P class="title2-style">' + CONVERT([varchar](20), ISNULL(@reportBuildStartTime, CONVERT([datetime], N'1900-01-01', 120)), 120) + N' (UTC)</P></TD>							
						</TR>' + 
						CASE WHEN @reportDescription IS NOT NULL
							 THEN N'
									<TR>
										<TD ALIGN=RIGHT WIDTH="60px"><P class="title3-style">&nbsp;</P></TD>	
										<TD ALIGN=LEFT  WIDTH="340px"><P class="title4-style">' + @reportDescription + N'</P></TD>
									</TR>'
							 ELSE N''
						END + 
						N'
					</TABLE>
				</TD>
			</TR>
			</TABLE>
		</TD>
		<TD WIDTH="430px"ALIGN=RIGHT VALIGN="TOP">
			<TABLE CELLSPACING=0 CELLPADDING="3px" border=0 WIDTH="430px"> 
				<TR VALIGN=TOP>
					<TD WIDTH="170px" ALIGN=CENTER>' + [dbo].[ufn_reportHTMLGetImage]('Logo') + N'</TD>	
					<TD WIDTH="260px" ALIGN=CENTER><P class="title2-style" ALIGN=CENTER>dbaTDPMon<br>Daily Health Check Report</P></TD>
				</TR>
				<TR VALIGN=TOP>
					<TD  COLSPAN="2" ALIGN=RIGHT><P class="disclaimer"><BR><A TARGET="_blank" HREF="https://github.com/rentadba/dbaTDPMon">https://github.com/rentadba/dbaTDPMon</A>, under  MIT License model
																		<BR>version ' + @HTMLReportArea + N'
																	</P></TD>
				</TR>
			</TABLE>
		</TD>
	</TR>
	</TABLE>
	<HR WIDTH="1130px" ALIGN=LEFT><br>'

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
					<TD ALIGN=CENTER class="summary-style add-border color-1">
						<A HREF="#ErrorlogMessagesIssuesDetected" class="summary-style color-1">Issues Detected {ErrorlogMessagesIssuesDetectedCount}</A>
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
						  THEN N'<A HREF="#OSEventMessagesIssuesDetected" class="summary-style color-2">Issues Detected {OSEventMessagesIssuesDetectedCount}</A>'
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
						  THEN N'<A HREF="#DatabasesStatusIssuesDetected" class="summary-style color-1">Inaccessible Databases {DatabasesStatusIssuesDetectedCount}</A>'
						  ELSE N'Offline Databases (N/A)'
					END + N'
					</TD>
					<TD ALIGN=LEFT class="summary-style add-border color-1">' +
					CASE WHEN (@flgActions & 2 = 2) AND (@flgOptions & 8192 = 8192)
						  THEN N'<A HREF="#DatabaseBACKUPAgeIssuesDetected" class="summary-style color-1">Outdated Backup for Databases {DatabaseBACKUPAgeIssuesDetectedCount}</A>'
						  ELSE N'Outdated Backup for Databases (N/A)'
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
					CASE WHEN (@flgActions & 2 = 2) AND (@flgOptions & 16384 = 16384)
						  THEN N'<A HREF="#DatabaseDBCCCHECKDBAgeIssuesDetected" class="summary-style color-2">Outdated DBCC CHECKDB Databases {DatabaseDBCCCHECKDBAgeIssuesDetectedCount}</A>'
						  ELSE N'Outdated DBCC CHECKDB Databases (N/A)'
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
					CASE WHEN (@flgActions & 2 = 2) AND (@flgOptions & 16777216 = 16777216)
						  THEN N'<A HREF="#FrequentlyFragmentedIndexesIssuesDetected" class="summary-style color-2">Frequently Fragmented Indexes {FrequentlyFragmentedIndexesIssuesDetectedCount}</A>'
						  ELSE N'Frequently Fragmented Indexes (N/A)'
					END + N'
					</TD>
					<TD ALIGN=LEFT class="summary-style add-border color-2">' +
					CASE WHEN (@flgActions & 2 = 2) AND (@flgOptions & 8388608 = 8388608)
						  THEN N'<A HREF="#DatabasePageVerifyIssuesDetected" class="summary-style color-2">Databases with Improper Page Verify Option {DatabasePageVerifyIssuesDetectedCount}</A>'
						  ELSE N'Databases with Improper Page Verify Option (N/A)'
					END + N'
					</TD>
				</TR>
				<TR VALIGN="TOP" class="color-1">
					<TD ALIGN=LEFT class="summary-style add-border color-1">' +
					CASE WHEN (@flgActions & 16 = 16) AND (@flgOptions & 536870912 = 536870912)
						  THEN N'<A HREF="#FailedLoginsAttemptsIssuesDetected" class="summary-style color-1">Failed Login Attempts {FailedLoginsAttemptsIssuesDetectedCount}</A>'
						  ELSE N'Failed Logins Attempts (N/A)'
					END + N'
					</TD>
					<TD ALIGN=LEFT class="summary-style add-border color-1">' +
					CASE WHEN (@flgActions & 2 = 2) AND (@flgOptions & 4194304 = 4194304)
						  THEN N'<A HREF="#DatabaseFixedFileSizeIssuesDetected" class="summary-style color-1">Databases with Fixed File(s) Size {DatabaseFixedFileSizeIssuesDetectedCount}</A>'
						  ELSE N'Databases with Fixed File(s) Size (N/A)'
					END + N'
					</TD>
				</TR>
			</table>
			<br>
			<table CELLSPACING=0 CELLPADDING="3px" border=0 width="552px" class="with-border">
				<TR VALIGN="TOP" class="color-3">
					<TD ALIGN=LEFT class="summary-style-title add-border color-3" colspan="2">Capacity analysis</TD>
				</TR>
				<TR VALIGN="TOP" class="color-1">
					<TD ALIGN=LEFT class="summary-style add-border color-1">' +
					CASE WHEN (@flgActions & 8 = 8) AND (@flgOptions & 262144 = 262144)
						  THEN N'<A HREF="#DiskSpaceInformationIssuesDetected" class="summary-style color-1">Low Free Disk Space {DiskSpaceInformationIssuesDetectedCount}</A>'
						  ELSE N'Low Free Disk Space (N/A)'
					END + N'
					</TD>
					<TD ALIGN=LEFT class="summary-style add-border color-1">' +
					CASE WHEN (@flgActions & 2 = 2) AND (@flgOptions & 2048 = 2048)
						  THEN N'<A HREF="#DatabaseMinDataSpaceIssuesDetected" class="summary-style color-1">Low Usage of Data Space {DatabaseMinDataSpaceIssuesDetectedCount}</A>'
						  ELSE N'Low Usage of Data Space (N/A)'
					END + N'
					</TD>
				</TR>
				<TR VALIGN="TOP" class="color-2">
					<TD ALIGN=LEFT class="summary-style add-border color-2">' +
					CASE WHEN (@flgActions & 2 = 2)  AND (@flgOptions & 128 = 128)
						  THEN N'<A HREF="#SystemDatabasesSizeIssuesDetected" class="summary-style color-2">Big Size for System Databases {SystemDatabasesSizeIssuesDetectedCount}</A>'
						  ELSE N'Big Size for System Databases (N/A)'
					END + N'
					</TD>
					<TD ALIGN=LEFT class="summary-style add-border color-2">' +
					CASE WHEN (@flgActions & 2 = 2) AND (@flgOptions & 32768 = 32768)
						  THEN N'<A HREF="#DatabaseMaxLogSpaceIssuesDetected" class="summary-style color-2">High Usage of Log Space {DatabaseMaxLogSpaceIssuesDetectedCount}</A>'
						  ELSE N'High Usage of Log Space (N/A)'
					END + N'
					</TD>
				</TR>
				<TR VALIGN="TOP" class="color-1">
					<TD ALIGN=LEFT class="summary-style add-border color-1">' +
					CASE WHEN (@flgActions & 2 = 2) AND (@flgOptions & 1024 = 1024)
						  THEN N'<A HREF="#DatabaseMaxLogSizeIssuesDetected" class="summary-style color-1">Big Size for Database Log files {DatabaseMaxLogSizeIssuesDetectedCount}</A>'
						  ELSE N'Big Size for Database Log files (N/A)'
					END + N'
					</TD>
					<TD ALIGN=LEFT class="summary-style add-border color-1">' +
					CASE WHEN (@flgActions & 2 = 2) AND (@flgOptions & 4096 = 4096)
						  THEN N'<A HREF="#DatabaseLogVsDataSizeIssuesDetected" class="summary-style color-1">Log vs. Data - Allocated Size {DatabaseLogVsDataSizeIssuesDetectedCount}</A>'
						  ELSE N'Log vs. Data - Allocated Size (N/A)'
					END + N'
					</TD>
				</TR>
				<TR VALIGN="TOP" class="color-1">
					<TD ALIGN=LEFT class="summary-style add-border color-1">' +
					CASE WHEN (@flgActions & 2 = 2) AND (@flgOptions & 1073741824 = 1073741824)
						  THEN N'<A HREF="#DatabaseGrowthIssuesDetected" class="summary-style color-1">Database(s) Growth {DatabaseGrowthIssuesDetectedCount}</A>'
						  ELSE N'Database(s) Growth (N/A)'
					END + N'
					</TD>
					<TD ALIGN=LEFT class="summary-style add-border color-1">' +
					CASE WHEN (@flgActions & 2 = 2)
						  THEN N'<A HREF="#BackupSizeDetails" class="summary-style color-1">Backup(s) Size Details</A>'
						  ELSE N'Backup(s) Size Details (N/A)'
					END + N'
					</TD>
				</TR>
			</table>

		</TD>
	</TR>
	</TABLE>			
	<HR WIDTH="1130px" ALIGN=LEFT><br>'

	-----------------------------------------------------------------------------------------------------
	--prepare data for the report. apply filters where needed. build temporary tables
	-----------------------------------------------------------------------------------------------------
	DECLARE @tmpProjectFilter [varchar](32)
	
	IF (@flgActions & 1 = 1) AND (@flgOptions & 2 = 2)
		begin
			SET @ErrMessage = 'analyzing database details - size and backup info'
			EXEC [dbo].[usp_logPrintMessage] @customMessage = @ErrMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 1, @messageTreelevel = 1, @stopExecution=0

			IF @reportOptionGetProjectDBSize = 1
				SET @tmpProjectFilter = @projectCode
			ELSE
				IF @reportOptionGetProjectDBSize = 0 OR (@flgOptions & 268435456 = 268435456)
					SET @tmpProjectFilter = '%'
			
			IF OBJECT_ID('tempdb..#hcReportCapacityDatabaseBackups') IS NOT NULL DROP TABLE #hcReportCapacityDatabaseBackups
			CREATE TABLE #hcReportCapacityDatabaseBackups
			(
				[instance_name]			[sysname]		NULL,
				[solution_name]			[nvarchar](128)	NULL,
				[is_production]			[bit]			NULL,
				[database_count]		[int]			NULL,
				[database_size_gb]		[numeric](18,3)	NULL,
				[backup_size_gb]		[numeric](18,3)	NULL,
				[backup_files_count]	[int]			NULL,
				[full_backup_gb]		[numeric](18,3)	NULL,
				[diff_backup_gb]		[numeric](18,3)	NULL,
				[log_backup_gb]			[numeric](18,3)	NULL
			)

			INSERT INTO #hcReportCapacityDatabaseBackups([instance_name], [solution_name], [is_production], [database_count], [database_size_gb], 
														 [backup_size_gb], [backup_files_count], [full_backup_gb], [diff_backup_gb], [log_backup_gb])
			EXEC [dbo].[usp_hcReportCapacityDatabaseBackups]	@projectCode		= @tmpProjectFilter,
																@sqlServerNameFilter= @sqlServerNameFilter,
																@daysToAnalyze		= @reportOptionGetBackupSizeLastDays
		end

	IF (@flgActions & 2 = 2) AND (@flgOptions & 1073741824 = 1073741824)
		begin
			SET @ErrMessage = 'analyzing database details - growth'
			EXEC [dbo].[usp_logPrintMessage] @customMessage = @ErrMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 1, @messageTreelevel = 1, @stopExecution=0

			IF @reportOptionGetProjectDBSize = 1
				SET @tmpProjectFilter = @projectCode
			ELSE
				IF @reportOptionGetProjectDBSize = 0 OR (@flgOptions & 268435456 = 268435456)
					SET @tmpProjectFilter = '%'

			IF OBJECT_ID('tempdb..#hcReportCapacityDatabaseGrowth') IS NOT NULL DROP TABLE #hcReportCapacityDatabaseGrowth
			CREATE TABLE #hcReportCapacityDatabaseGrowth
				(
					  [instance_name]			[sysname]
					, [database_name]			[sysname]
					, [current_size_mb]			[numeric](18,3)
					, [old_size_mb]				[numeric](18,3)
					, [current_data_size_mb]	[numeric](18,3)
					, [old_data_size_mb]		[numeric](18,3)
					, [current_log_size_mb]		[numeric](18,3)
					, [old_log_size_mb]			[numeric](18,3)
					, [growth_size_mb]			[numeric](18,3)
					, [data_growth_percent]		[numeric](18,3)
				)			

			INSERT	INTO #hcReportCapacityDatabaseGrowth([instance_name], [database_name], [current_size_mb], [old_size_mb], [current_data_size_mb], [old_data_size_mb],
														 [current_log_size_mb], [old_log_size_mb], [growth_size_mb], [data_growth_percent])
					EXEC [dbo].[usp_hcReportCapacityDatabaseGrowth]	@projectCode		 = @tmpProjectFilter,
																	@sqlServerNameFilter = @sqlServerNameFilter,
																	@daysToAnalyze		 = @reportOptionGetDBGrowthLastDays
		end

	IF (@flgActions & 16 = 16) AND (@flgOptions & 1048576 = 1048576)
		begin
			SET @ErrMessage = 'analyzing errorlog messages - filtering messages'
			EXEC [dbo].[usp_logPrintMessage] @customMessage = @ErrMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 1, @messageTreelevel = 1, @stopExecution=0

			IF OBJECT_ID('tempdb..#filteredStatsSQLServerErrorlogDetail') IS NOT NULL
				DROP TABLE #filteredStatsSQLServerErrorlogDetail

			SET @dateTimeLowerLimit		= DATEADD(hh, -@reportOptionErrorlogMessageLastHours, GETDATE())
			SET @dateTimeLowerLimitUTC	= DATEADD(hh, -@reportOptionErrorlogMessageLastHours, GETUTCDATE())

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
			WHERE	cin.[instance_active]=1
					AND (cin.[project_id]=@projectID OR (@flgOptions & 268435456 = 268435456))
					AND cin.[instance_name] LIKE @sqlServerNameFilter
					AND (   (eld.[log_date_utc] IS NOT NULL AND eld.[log_date_utc] >= @dateTimeLowerLimitUTC)
						 OR (eld.[log_date_utc] IS NULL     AND eld.[log_date] >= @dateTimeLowerLimit)
						)
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

			SET @ErrMessage = 'done'
			EXEC [dbo].[usp_logPrintMessage] @customMessage = @ErrMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 1, @messageTreelevel = 1, @stopExecution=0
		end

	IF (@flgActions & 16 = 16) AND (@flgOptions & 536870912 = 536870912)
		begin
			SET @ErrMessage = 'analyzing errorlog messages - failed login attempts'
			EXEC [dbo].[usp_logPrintMessage] @customMessage = @ErrMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 1, @messageTreelevel = 1, @stopExecution=0

			IF OBJECT_ID('tempdb..#filteredStatsFailedLoginsDetails') IS NOT NULL
				DROP TABLE #filteredStatsFailedLoginsDetails

			SET @dateTimeLowerLimit		= DATEADD(hh, -@reportOptionErrorlogMessageLastHours, GETDATE())
			SET @dateTimeLowerLimitUTC	= DATEADD(hh, -@reportOptionErrorlogMessageLastHours, GETUTCDATE())

			SELECT DISTINCT eld.[instance_name], eld.[log_date], eld.[id], eld.[text], eld.[event_date_utc]
			INTO #filteredStatsFailedLoginsDetails
			FROM [dbo].[vw_catalogInstanceNames]  cin
			INNER JOIN [health-check].[vw_statsErrorlogDetails]	eld	ON eld.[project_id] = cin.[project_id] AND eld.[instance_id] = cin.[instance_id]
			LEFT JOIN [report].[htmlSkipRules] rsr ON	rsr.[module] = 'health-check'
														AND rsr.[rule_id] = 536870912
														AND rsr.[active] = 1
														AND (rsr.[skip_value] = cin.[machine_name] OR rsr.[skip_value]=cin.[instance_name])
			WHERE	cin.[instance_active]=1
					AND (cin.[project_id]=@projectID OR (@flgOptions & 268435456 = 268435456))
					AND cin.[instance_name] LIKE @sqlServerNameFilter
					AND (   (eld.[log_date_utc] IS NOT NULL AND eld.[log_date_utc] >= @dateTimeLowerLimitUTC)
						 OR (eld.[log_date_utc] IS NULL     AND eld.[log_date] >= @dateTimeLowerLimit)
						)
					AND eld.[process_info] = 'Logon'
					AND eld.[text] LIKE '%failed%'			
					AND rsr.[id] IS NULL

			IF OBJECT_ID('tempdb..#filteredStatsFailedLoginsAttempts') IS NOT NULL
				DROP TABLE #filteredStatsFailedLoginsAttempts

			SELECT DISTINCT [instance_name], REPLACE([login_name], '''', '') as [login_name], [reason]
					, COUNT(*) as [occurencies]
			INTO #filteredStatsFailedLoginsAttempts
			FROM (
					SELECT	[instance_name],
							SUBSTRING([text], CHARINDEX('Reason: ', [text]), LEN([text]) - CHARINDEX('Reason: ', [text]) + 1) AS [reason],
							SUBSTRING([text],  CHARINDEX('''', [text]), (CHARINDEX('''', [text],  CHARINDEX('''', [text]) + 1)) - CHARINDEX('''', [text])) AS [login_name],
							[text]				
					FROM #filteredStatsFailedLoginsDetails	
					WHERE	CHARINDEX('.', [text], CHARINDEX('Reason: ', [text]) + 8) - CHARINDEX('Reason: ', [text]) > 0
							AND CHARINDEX('''', [text], CHARINDEX('''', [text]) + 1) - CHARINDEX('''', [text]) > 0
				) x 
			GROUP BY [instance_name], [login_name], [reason]

			CREATE INDEX IX_filteredStatsFailedLoginsAttempts_InstanceName ON #filteredStatsFailedLoginsAttempts([instance_name])

			SET @ErrMessage = 'done'
			EXEC [dbo].[usp_logPrintMessage] @customMessage = @ErrMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 1, @messageTreelevel = 1, @stopExecution=0
		end

	IF (@flgActions & 32 = 32) AND (@flgOptions & 134217728 = 134217728)
		begin
			SET @ErrMessage = 'analyzing os event messages'
			EXEC [dbo].[usp_logPrintMessage] @customMessage = @ErrMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 1, @messageTreelevel = 1, @stopExecution=0

			IF OBJECT_ID('tempdb..#filteredStatsOSEventMessagesDetail') IS NOT NULL
				DROP TABLE #filteredStatsOSEventMessagesDetail

			SET @dateTimeLowerLimit		= DATEADD(hh, -@reportOptionOSEventMessageLastHours, GETDATE())
			SET @dateTimeLowerLimitUTC	= DATEADD(hh, -@reportOptionOSEventMessageLastHours, GETUTCDATE())

			SELECT DISTINCT
					oel.[machine_name], CONVERT([datetime], oel.[time_created]) [time_created], oel.[log_type_desc], oel.[level_desc], 
					oel.[event_id], oel.[record_id], oel.[source], oel.[message]
			INTO #filteredStatsOSEventMessagesDetail
			FROM [dbo].[vw_catalogInstanceNames]	cin
			INNER JOIN [health-check].[vw_statsOSEventLogs]	oel	ON oel.[project_id] = cin.[project_id] AND oel.[instance_id] = cin.[instance_id]
			LEFT JOIN [report].[htmlSkipRules] rsr ON	rsr.[module] = 'health-check'
														AND rsr.[rule_id] = 134217728
														AND rsr.[active] = 1
														AND (rsr.[skip_value] = cin.[machine_name] OR rsr.[skip_value]=cin.[instance_name])
			WHERE	cin.[instance_active]=1
					AND (cin.[project_id]=@projectID OR (@flgOptions & 268435456 = 268435456))
					AND cin.[instance_name] LIKE @sqlServerNameFilter
					AND (   (oel.[time_created_utc] IS NOT NULL AND oel.[time_created_utc] >= @dateTimeLowerLimitUTC)
						 OR (oel.[time_created_utc] IS NULL     AND CONVERT([datetime], oel.[time_created]) >= @dateTimeLowerLimit)
						)
					AND NOT EXISTS	( 
										SELECT 1
										FROM	[report].[hardcodedFilters] chf 
										WHERE	chf.[module] = 'health-check'
												AND chf.[object_name] = 'statsOSEventLogs'
												AND chf.[active] = 1
												AND PATINDEX(chf.[filter_pattern], oel.[message]) > 0
									)
					AND rsr.[id] IS NULL
			
			CREATE INDEX IX_filteredStatsOSEventMessagesDetail_MachineName ON #filteredStatsOSEventMessagesDetail([machine_name])

			SET @ErrMessage = 'done'
			EXEC [dbo].[usp_logPrintMessage] @customMessage = @ErrMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 1, @messageTreelevel = 1, @stopExecution=0
		end

	-----------------------------------------------------------------------------------------------------
	--Offline Instances
	-----------------------------------------------------------------------------------------------------
	IF (@flgActions & 1 = 1) AND (@flgOptions & 1 = 1)
		begin
			SET @ErrMessage='Build Report: Instance Availability - Offline'
			EXEC [dbo].[usp_logPrintMessage] @customMessage = @ErrMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0
			
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

			DECLARE crsInstancesOffline CURSOR LOCAL FAST_FORWARD FOR	SELECT  DISTINCT 
																				  cin.[machine_name], cin.[instance_name]
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
																				AND (cin.[project_id]=@projectID OR (@flgOptions & 268435456 = 268435456))
																				AND cin.[instance_name] LIKE @sqlServerNameFilter
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
										N'<TD WIDTH="120px" class="details" ALIGN="LEFT" nowrap>' + CASE WHEN @isClustered=0 THEN @machineName ELSE dbo.ufn_reportHTMLGetClusterNodeNames(@projectID, @instanceName, @flgOptions) END + N'</TD>' + 
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
	ELSE
		SET @HTMLReport = REPLACE(@HTMLReport, '{InstancesOfflineCount}', '(N/A)')


	-----------------------------------------------------------------------------------------------------
	--Online Instances
	-----------------------------------------------------------------------------------------------------
	IF (@flgActions & 1 = 1) AND (@flgOptions & 2 = 2)
		begin
			SET @ErrMessage = 'Build Report: Instance Availability - Online'
			EXEC [dbo].[usp_logPrintMessage] @customMessage = @ErrMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0
			
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
											<TH WIDTH="180px" class="details-bold">Instance Name</TH>
											<TH WIDTH=" 80px" class="details-bold">Clustered</TH>
											<TH WIDTH= "80px" class="details-bold" nowrap >Version</TH>
											<TH WIDTH="240px" class="details-bold">Edition</TH>
											<TH WIDTH= "80px" class="details-bold" nowrap>DB Size (GB)</TH>
											<TH WIDTH= "80px" class="details-bold" nowrap>DB Count</TH>
											<TH WIDTH="140px" class="details-bold" nowrap>Refresh Date (UTC)</TH>'

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

			DECLARE crsInstancesOnline CURSOR LOCAL FAST_FORWARD FOR	SELECT DISTINCT  
																				  cin.[machine_name], cin.[instance_name]
																				, cin.[is_clustered], cin.[cluster_node_machine_name]
																				, MAX(cin.[version]) AS [version]
																				, MAX(cin.[edition]) AS [edition]
																				, MAX(cin.[last_refresh_date_utc]) AS [last_refresh_date_utc]
																				, MAX(shcdd.[database_count])   AS [database_count]
																				, MAX(shcdd.[db_size_gb])		AS [db_size_gb]
																		FROM [dbo].[vw_catalogInstanceNames]  cin
																		LEFT JOIN 
																			(
																				SELECT	[instance_name], 
																						SUM([database_count])	AS [database_count],
																						SUM([database_size_gb]) AS [db_size_gb]
																				FROM #hcReportCapacityDatabaseBackups
																				GROUP BY [instance_name]
																			) shcdd ON	shcdd.[instance_name] = cin.[instance_name] COLLATE DATABASE_DEFAULT
																		LEFT JOIN [report].[htmlSkipRules] rsr ON	rsr.[module] = 'health-check'
																													AND rsr.[rule_id] = 2
																													AND rsr.[active] = 1
																													AND (rsr.[skip_value] = cin.[machine_name] OR rsr.[skip_value]=cin.[instance_name])
																		WHERE cin.[instance_active]=1
																				AND (cin.[project_id]=@projectID OR (@flgOptions & 268435456 = 268435456))
																				AND cin.[instance_name] LIKE @sqlServerNameFilter
																				AND rsr.[id] IS NULL
																		GROUP BY  cin.[machine_name], cin.[instance_name]
																				, cin.[is_clustered], cin.[cluster_node_machine_name]																				
																		ORDER BY cin.[instance_name], cin.[machine_name]
			OPEN crsInstancesOnline
			FETCH NEXT FROM crsInstancesOnline INTO @machineName, @instanceName, @isClustered, @clusterNodeName, @version, @edition, @lastRefreshDate, @dbCount, @dbSize
			WHILE @@FETCH_STATUS=0
				begin
					SET @hasDatabaseDetails = 0
					SELECT	@hasDatabaseDetails = COUNT(*)
					FROM	[dbo].[vw_catalogDatabaseNames]
					WHERE	([project_id]=@projectID OR (@flgOptions & 268435456 = 268435456))
							AND [instance_name] = @instanceName

					SET @hasSQLagentJob = 0
					SELECT	@hasSQLagentJob = COUNT(*)
					FROM	[health-check].[vw_statsSQLAgentJobsHistory]
					WHERE	([project_id]=@projectID OR (@flgOptions & 268435456 = 268435456))
							AND [instance_name] = @instanceName

					SET @hasDiskSpaceInfo = 0
					SELECT	@hasDiskSpaceInfo = COUNT(*)
					FROM	[health-check].[vw_statsDiskSpaceInfo]
					WHERE	([project_id]=@projectID OR (@flgOptions & 268435456 = 268435456))
							AND [instance_name] = @instanceName
					
					SET @hasErrorlogMessages = 0
					SELECT	@hasErrorlogMessages = COUNT(*)
					FROM	#filteredStatsSQLServerErrorlogDetail
					WHERE	[instance_name] = @instanceName

					SET @hasOSEventMessages = 0
					SELECT	@hasOSEventMessages = COUNT(*)
					FROM	#filteredStatsOSEventMessagesDetail
					WHERE	[machine_name] = @machineName
																				  

					SET @HTMLReportArea = @HTMLReportArea + 
								N'<TR VALIGN="CENTER" class="' + CASE WHEN @idx & 1 = 1 THEN 'color-2' ELSE 'color-1' END + '">' + 
										N'<TD WIDTH="100px" class="details" ALIGN="CENTER">' + 
										CASE	WHEN @flgActions & 2 = 2 AND @flgOptions & 8 = 8 AND @hasDatabaseDetails<>0
												THEN N'<BR><A HREF="#DatabasesStatusCompleteDetails' + @instanceName + N'">Databases</A>'
												ELSE N''
										END +
										CASE WHEN @flgActions & 4 = 4 AND @flgOptions & 64 = 64 AND @hasSQLagentJob<>0
												THEN N'<BR><A HREF="#SQLServerAgentJobsStatusCompleteDetails' + @instanceName + N'">SQL Agent Jobs</A>'
												ELSE N''
										END +
										CASE WHEN @flgActions & 8 = 8 AND @flgOptions & 65536 = 65536 AND @hasDiskSpaceInfo<>0
												THEN N'<BR><A HREF="#DiskSpaceInformationCompleteDetails' + CASE WHEN @isClustered=0 THEN @machineName ELSE @clusterNodeName END + N'">Disk Space</A>'
												ELSE N''
										END +  
										CASE WHEN @flgActions & 16 = 16 AND @flgOptions & 1048576 = 1048576 AND @hasErrorlogMessages<>0
												THEN N'<BR><A HREF="#ErrorlogMessagesIssuesDetected' + @instanceName + N'">Errorlog</A>'
												ELSE N''
										END +  
										CASE WHEN @flgActions & 32 = 32 AND @flgOptions & 134217728 = 134217728 AND @hasOSEventMessages<>0
												THEN N'<BR><A HREF="#OSEventMessagesIssuesDetected' + @machineName + N'">OS Events</A>'
												ELSE N''
										END +  
											N'<BR><BR>
										</TD>' + 
										N'<TD WIDTH="150px" class="details" ALIGN="LEFT">' + CASE WHEN @isClustered=0 THEN @machineName ELSE dbo.ufn_reportHTMLGetClusterNodeNames(@projectID, @instanceName, @flgOptions) END + N'</TD>' + 
										N'<TD WIDTH="180px" class="details" ALIGN="LEFT">' + @instanceName + N'</TD>' + 
										N'<TD WIDTH=" 80px" class="details" ALIGN="CENTER">' + CASE WHEN @isClustered=0 THEN N'No' ELSE N'Yes' + ISNULL(N'<BR>[' + @clusterNodeName + ']', N'&nbsp;') END + N'</TD>' + 
										N'<TD WIDTH= "80px" class="details" ALIGN="CENTER" nowrap>' + ISNULL(@version, N'&nbsp;') + N'</TD>' + 
										N'<TD WIDTH="240px" class="details" ALIGN="LEFT">' + ISNULL([dbo].[ufn_reportHTMLPrepareText](@edition, 0), N'&nbsp;') + N'</TD>' + 
										N'<TD WIDTH= "80px" class="details" ALIGN="RIGHT" nowrap>' + ISNULL(CAST(@dbSize AS [nvarchar](64)), N'&nbsp;') + N'</TD>' + 
										N'<TD WIDTH= "80px" class="details" ALIGN="RIGHT" nowrap>' + ISNULL(CAST(@dbCount AS [nvarchar](64)), N'&nbsp;') + N'</TD>' + 
										N'<TD WIDTH="140px" class="details" ALIGN="LEFT" nowrap>' + ISNULL(CONVERT([nvarchar](24), @lastRefreshDate, 121), N'&nbsp;') + N'</TD>' + 
									N'</TR>'
					SET @idx=@idx+1

					FETCH NEXT FROM crsInstancesOnline INTO @machineName, @instanceName, @isClustered, @clusterNodeName, @version, @edition, @lastRefreshDate, @dbCount, @dbSize
				end
			CLOSE crsInstancesOnline
			DEALLOCATE crsInstancesOnline

			SET @HTMLReportArea = @HTMLReportArea + N'</TABLE>
								</TD>
							</TR>
						</TABLE>'

			SET @HTMLReportArea = @HTMLReportArea + N'<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="3px"><TR><TD WIDTH="1130px" ALIGN=RIGHT><A HREF="#Home" class="normal">Go Up</A></TD></TR></TABLE>'	
			SET @HTMLReport = @HTMLReport + @HTMLReportArea					

			SET @HTMLReport = REPLACE(@HTMLReport, '{InstancesOnlineCount}', '(' + CAST((@idx-1) AS [nvarchar]) + ')')

			/* AlwaysOn Availability Group details*/
			SET @ErrMessage = 'Build Report: Instance Availability - Online - AG details'
			EXEC [dbo].[usp_logPrintMessage] @customMessage = @ErrMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0
			
			IF EXISTS(	SELECT	DISTINCT
								  sdad.[cluster_name], sdad.[ag_name], sdad.[instance_name]
								, sdad.[replica_connected_state_desc], sdad.[replica_join_state_desc], sdad.[role_desc]
								, sdad.[failover_mode_desc], sdad.[availability_mode_desc], sdad.[readable_secondary_replica], sdad.[synchronization_health_desc]
								, sdad.[database_name], sdad.[synchronization_state_desc], sdad.[suspend_reason_desc]
						FROM [health-check].[vw_statsDatabaseAlwaysOnDetails] sdad
						LEFT JOIN [report].[htmlSkipRules] rsr ON	rsr.[module] = 'health-check'
																AND rsr.[rule_id] = 2
																AND rsr.[active] = 1
																AND (rsr.[skip_value] = sdad.[machine_name] OR rsr.[skip_value]=sdad.[instance_name])
																AND (	rsr.[skip_value2] IS NULL
																		OR rsr.[skip_value2] = sdad.[ag_name]
																	)
						WHERE sdad.[instance_active]=1
								AND (sdad.[project_id]=@projectID OR (@flgOptions & 268435456 = 268435456))
								AND sdad.[instance_name] LIKE @sqlServerNameFilter
								AND rsr.[id] IS NULL
					 ) 
				begin
					SET @HTMLReportArea=N''
					SET @HTMLReportArea = @HTMLReportArea + 
									N'<A NAME="AlwaysOnAvailabilityGroupsDetails" class="category-style">AlwaysOn Availability Groups Details</A><br>
									<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="0px" class="no-border">
									<TR VALIGN=TOP>
										<TD WIDTH="1130px">
											<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="3px" class="with-border">' +
												N'<TR class="color-3">
													<TH WIDTH="120px" class="details-bold" nowrap>Cluster Name</TH>
													<TH WIDTH="120px" class="details-bold">AG Name</TH>
													<TH WIDTH="150px" class="details-bold">Instance Name</TH>
													<TH WIDTH=" 80px" class="details-bold">Replica Role</TH>
													<TH WIDTH="220px" class="details-bold" nowrap >Properties</TH>
													<TH WIDTH="180px" class="details-bold">Database Name</TH>
													<TH WIDTH= 120px" class="details-bold" nowrap>Status</TH>
													<TH WIDTH="140px" class="details-bold" nowrap>Refresh Date (UTC)</TH>'
		
					DECLARE   @clusterName		[varchar](256)
							, @agName			[varchar](256)
							, @roleDesc			[sysname]
							, @agProperties		[nvarchar](max)
							, @counter			[int]
							, @dbName			[sysname]
							, @syncStatus		[varchar](256)

					SET @idx=1		

					DECLARE crsAGDetails CURSOR LOCAL FAST_FORWARD FOR	SELECT [cluster_name], [ag_name], [instance_name], [role_desc], [properties], COUNT(DISTINCT [database_name]) cnt
																		FROM (
																				SELECT	DISTINCT
																							  sdad.[cluster_name]
																							, sdad.[ag_name]
																							, sdad.[instance_name]
																							, sdad.[role_desc]
																							, [database_name]
																							, N'connected state: ' + sdad.[replica_connected_state_desc] + N'<br>' + 
																							  N'join state: ' + sdad.[replica_join_state_desc] + N'<br>' + 
																							  N'failover mode: ' + sdad.[failover_mode_desc] + N'<br>' + 
																							  N'availability mode: ' + sdad.[availability_mode_desc] + N'<br>' + 
																							  N'readable secondary: ' + sdad.[readable_secondary_replica] + N'<br>' + 
																							  N'synchronization health: ' + sdad.[synchronization_health_desc] + N'<br>' AS [properties]
																					FROM [health-check].[vw_statsDatabaseAlwaysOnDetails] sdad
																					LEFT JOIN [report].[htmlSkipRules] rsr ON	rsr.[module] = 'health-check'
																															AND rsr.[rule_id] = 2
																															AND rsr.[active] = 1
																															AND (rsr.[skip_value] = sdad.[machine_name] OR rsr.[skip_value]=sdad.[instance_name])
																															AND (	rsr.[skip_value2] IS NULL
																																	OR rsr.[skip_value2] = sdad.[ag_name]
																																)
																					WHERE	(sdad.[project_id]=@projectID OR (@flgOptions & 268435456 = 268435456))
																							AND sdad.[instance_name] LIKE @sqlServerNameFilter
																							AND rsr.[id] IS NULL
																				)X
																		GROUP BY [cluster_name], [ag_name], [instance_name], [role_desc], [properties]
																		ORDER BY [cluster_name], [ag_name], [role_desc], [instance_name]
					OPEN crsAGDetails
					FETCH NEXT FROM crsAGDetails INTO @clusterName, @agName, @instanceName, @roleDesc, @agProperties, @counter
					WHILE @@FETCH_STATUS=0
						begin
							SET @HTMLReportArea = @HTMLReportArea + 
										N'<TR VALIGN="CENTER" class="' + CASE WHEN @idx & 1 = 1 THEN 'color-2' ELSE 'color-1' END + '">' + 
												N'<TD WIDTH="120px" class="details" ALIGN="LEFT" ROWSPAN="' + CAST(@counter AS [nvarchar](64)) + N'">' + @clusterName + N'</TD>' + 
												N'<TD WIDTH="120px" class="details" ALIGN="LEFT" ROWSPAN="' + CAST(@counter AS [nvarchar](64)) + N'">' + @agName + N'</TD>' + 
												N'<TD WIDTH="180px" class="details" ALIGN="LEFT" ROWSPAN="' + CAST(@counter AS [nvarchar](64)) + N'">' + @instanceName + N'</TD>' + 
												N'<TD WIDTH=" 80px" class="details" ALIGN="LEFT" ROWSPAN="' + CAST(@counter AS [nvarchar](64)) + N'">' + @roleDesc + N'</TD>' + 
												N'<TD WIDTH="220px" class="details" ALIGN="LEFT" ROWSPAN="' + CAST(@counter AS [nvarchar](64)) + N'">' + @agProperties + N'</TD>'

							SET @tmpHTMLReport=N''
							SELECT @tmpHTMLReport=((
											SELECT	N'<TD WIDTH="180px" class="details" ALIGN="LEFT">' + [database_name] + N'</TD>' + 
													N'<TD WIDTH="120px" class="details" ALIGN="LEFT">' + [synchronization_state_desc] + N'</TD>' + 
													N'<TD WIDTH="140px" class="details" ALIGN="LEFT" nowrap>' + ISNULL(CONVERT([nvarchar](24), [event_date_utc], 121), N'&nbsp;') + N'</TD>' + 
												N'</TR>'+ 
													CASE WHEN [row_count] > [row_no]
														 THEN N'<TR VALIGN="TOP" class="' + CASE WHEN @idx & 1 = 1 THEN 'color-2' ELSE 'color-1' END + '">'
														 ELSE N''
													END
											FROM (
													SELECT    [event_date_utc], [database_name]
															, [synchronization_state_desc] + CASE WHEN [suspend_reason_desc] IS NOT NULL THEN N' (' + [suspend_reason_desc] + N')' ELSE N'' END [synchronization_state_desc]
															, ROW_NUMBER() OVER(ORDER BY [cluster_name], [ag_name], [instance_name]) [row_no]
															, SUM(1) OVER() AS [row_count]
													FROM	[health-check].[vw_statsDatabaseAlwaysOnDetails] sdad
													WHERE	[cluster_name] = @clusterName
														AND [ag_name] = @agName
														AND [instance_name] = @instanceName
														AND [role_desc] = @roleDesc
												)X
											ORDER BY [database_name]
											FOR XML PATH(''), TYPE
											).value('.', 'nvarchar(max)'))

							SET @idx=@idx+1
							SET @HTMLReportArea = @HTMLReportArea + COALESCE(@tmpHTMLReport, '')

							FETCH NEXT FROM crsAGDetails INTO @clusterName, @agName, @instanceName, @roleDesc, @agProperties, @counter
						end
					CLOSE crsAGDetails
					DEALLOCATE crsAGDetails

					SET @HTMLReportArea = @HTMLReportArea + N'</TABLE>
										</TD>
									</TR>
								</TABLE>'

					SET @HTMLReportArea = @HTMLReportArea + N'<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="3px"><TR><TD WIDTH="1130px" ALIGN=RIGHT><A HREF="#Home" class="normal">Go Up</A></TD></TR></TABLE>'	
					SET @HTMLReport = @HTMLReport + @HTMLReportArea					
				end
		end
	ELSE
		SET @HTMLReport = REPLACE(@HTMLReport, '{InstancesOnlineCount}', '(N/A)')


	-----------------------------------------------------------------------------------------------------
	--Databases Status - Permission Errors
	-----------------------------------------------------------------------------------------------------
	IF (@flgActions & 2 = 2) AND (@flgOptions & 256 = 256)
		begin
			SET @ErrMessage = 'Build Report: Databases Status - Permission Errors'
			EXEC [dbo].[usp_logPrintMessage] @customMessage = @ErrMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0
			
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
																					
			DECLARE crsDatabasesStatusPermissionErrors CURSOR LOCAL FAST_FORWARD FOR	SELECT    DISTINCT
																								  cin.[machine_name], cin.[instance_name]
																								, cin.[is_clustered], cin.[cluster_node_machine_name]
																								, COUNT(DISTINCT lsam.[message]) AS [message_count]
																						FROM [dbo].[vw_catalogInstanceNames]  cin
																						INNER JOIN [dbo].[vw_logAnalysisMessages] lsam ON lsam.[project_id] = cin.[project_id] AND lsam.[instance_id] = cin.[instance_id]
																						LEFT JOIN [report].[htmlSkipRules] rsr ON	rsr.[module] = 'health-check'
																																	AND rsr.[rule_id] = 256
																																	AND rsr.[active] = 1
																																	AND (rsr.[skip_value] = cin.[machine_name] OR rsr.[skip_value]=cin.[instance_name])
																						WHERE	cin.[instance_active]=1
																								AND (cin.[project_id]=@projectID OR (@flgOptions & 268435456 = 268435456))
																								AND cin.[instance_name] LIKE @sqlServerNameFilter
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
										N'<TD WIDTH="120px" class="details" ALIGN="LEFT" nowrap ROWSPAN="' + CAST(@messageCount AS [nvarchar](64)) + N'"><A NAME="DatabasesStatusPermissionErrors' + @instanceName + N'">' + CASE WHEN @isClustered=0 THEN @machineName ELSE dbo.ufn_reportHTMLGetClusterNodeNames(@projectID, @instanceName, @flgOptions) END + N'</A></TD>' + 
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
																	AND (cin.[project_id]=@projectID OR (@flgOptions & 268435456 = 268435456))
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
					AND (cin.[project_id]=@projectID OR (@flgOptions & 268435456 = 268435456))
					AND cin.[instance_name] LIKE @sqlServerNameFilter
					AND lsam.descriptor IN (N'dbo.usp_hcCollectDatabaseDetails')
					AND rsr.[id] IS NULL

			SET @HTMLReport = REPLACE(@HTMLReport, '{DatabasesStatusPermissionErrorsCount}', '(' + CAST((@idx) AS [nvarchar]) + ')')
		end
	ELSE
		SET @HTMLReport = REPLACE(@HTMLReport, '{DatabasesStatusPermissionErrorsCount}', '(N/A)')


	-----------------------------------------------------------------------------------------------------
	--Databases Status - Issues Detected
	-----------------------------------------------------------------------------------------------------
	IF (@flgActions & 2 = 2) AND (@flgOptions & 4 = 4)
		begin
			SET @ErrMessage = 'Build Report: Databases Status - Issues Detected'
			EXEC [dbo].[usp_logPrintMessage] @customMessage = @ErrMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0
			
			SET @HTMLReportArea=N''
			SET @HTMLReportArea = @HTMLReportArea + 
							N'<A NAME="DatabasesStatusIssuesDetected" class="category-style">Databases Status - Issues Detected</A><br>
							<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="0px" class="no-border">	
							<TR VALIGN=TOP>
								<TD class="small-size" COLLSPAN="5">database status not in (' + @reportOptionDatabaseAdmittedState + N')</TD>
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

			DECLARE crsDatabasesStatusIssuesDetected CURSOR LOCAL FAST_FORWARD FOR	SELECT DISTINCT
																							  cin.[machine_name], cin.[instance_name]
																							, cin.[is_clustered], cin.[cluster_node_machine_name]
																							, cdn.[database_name]
																							, cdn.[state_desc]
																					FROM [dbo].[vw_catalogInstanceNames]  cin
																					INNER JOIN [dbo].[vw_catalogDatabaseNames] cdn ON cdn.[project_id] = cin.[project_id] AND cdn.[instance_id] = cin.[instance_id]
																					LEFT JOIN [report].[htmlSkipRules] rsr ON	rsr.[module] = 'health-check'
																																AND rsr.[rule_id] = 4
																																AND rsr.[active] = 1
																																AND (rsr.[skip_value] = cin.[machine_name] OR rsr.[skip_value]=cin.[instance_name])
																																AND (	rsr.[skip_value2] IS NULL
																																		OR rsr.[skip_value2] = cdn.[database_name]
																																	)
																					WHERE cin.[instance_active]=1
																							AND cdn.[active]=1
																							AND (cin.[project_id] = @projectID OR (@flgOptions & 268435456 = 268435456))
																							AND cin.[instance_name] LIKE @sqlServerNameFilter
																							AND CHARINDEX(cdn.[state_desc], @reportOptionDatabaseAdmittedState)=0
																							AND rsr.[id] IS NULL
																					ORDER BY cin.[instance_name], cin.[machine_name], cdn.[database_name]
			OPEN crsDatabasesStatusIssuesDetected
			FETCH NEXT FROM crsDatabasesStatusIssuesDetected INTO @machineName, @instanceName, @isClustered, @clusterNodeName, @databaseName, @stateDesc
			WHILE @@FETCH_STATUS=0
				begin
					SET @HTMLReportArea = @HTMLReportArea + 
								N'<TR VALIGN="TOP" class="' + CASE WHEN @idx & 1 = 1 THEN 'color-2' ELSE 'color-1' END + '">' + 
										N'<TD WIDTH="120px" class="details" ALIGN="LEFT" nowrap>' + CASE WHEN @isClustered=0 THEN @machineName ELSE dbo.ufn_reportHTMLGetClusterNodeNames(@projectID, @instanceName, @flgOptions) END + N'</TD>' + 
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
	ELSE
		SET @HTMLReport = REPLACE(@HTMLReport, '{DatabasesStatusIssuesDetectedCount}', '(N/A)')


	-----------------------------------------------------------------------------------------------------
	--SQL Server Agent Jobs Status - Permission Errors
	-----------------------------------------------------------------------------------------------------
	IF (@flgActions & 4 = 4) AND (@flgOptions & 32 = 32)
		begin
			SET @ErrMessage = 'Build Report: SQL Server Agent Jobs Status - Permission Errors'
			EXEC [dbo].[usp_logPrintMessage] @customMessage = @ErrMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0
			
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

			DECLARE crsSQLServerAgentJobsStatusPermissionErrors CURSOR LOCAL FAST_FORWARD FOR	SELECT  DISTINCT
																										  cin.[machine_name], cin.[instance_name]
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
																										AND (cin.[project_id]=@projectID OR (@flgOptions & 268435456 = 268435456))
																										AND cin.[instance_name] LIKE @sqlServerNameFilter
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
										N'<TD WIDTH="120px" class="details" ALIGN="LEFT" nowrap>' + CASE WHEN @isClustered=0 THEN @machineName ELSE dbo.ufn_reportHTMLGetClusterNodeNames(@projectID, @instanceName, @flgOptions) END + N'</TD>' + 
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
	ELSE
		SET @HTMLReport = REPLACE(@HTMLReport, '{SQLServerAgentJobsStatusPermissionErrorsCount}', '(N/A)')


	-----------------------------------------------------------------------------------------------------
	--SQL Server Agent Jobs Status - Issues Detected
	-----------------------------------------------------------------------------------------------------
	IF (@flgActions & 4 = 4) AND (@flgOptions & 16 = 16)
		begin
			SET @ErrMessage = 'Build Report: SQL Server Agent Jobs Status - Issues Detected'
			EXEC [dbo].[usp_logPrintMessage] @customMessage = @ErrMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0
			
			SET @HTMLReportArea=N''
			SET @HTMLReportArea = @HTMLReportArea + 
							N'<A NAME="SQLServerAgentJobsStatusIssuesDetected" class="category-style">SQL Server Agent Jobs Status - Issues Detected (last ' + CAST(@reportOptionJobFailuresInLastHours AS [nvarchar]) + N'h)</A><br>
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
			
			SET @dateTimeLowerLimit		= DATEADD(hh, -@reportOptionJobFailuresInLastHours, GETDATE())
			SET @dateTimeLowerLimitUTC	= DATEADD(hh, -@reportOptionJobFailuresInLastHours, GETUTCDATE())

			DECLARE crsSQLServerAgentJobsStatusIssuesDetected CURSOR LOCAL FAST_FORWARD  FOR	SELECT	DISTINCT
																										ssajh.[instance_name], ssajh.[job_name], ssajh.[last_execution_status], ssajh.[last_execution_date], ssajh.[last_execution_time], ssajh.[message]
																								FROM	[health-check].[vw_statsSQLAgentJobsHistory] ssajh
																								INNER JOIN [dbo].[vw_catalogInstanceNames] cin ON cin.[project_id] = ssajh.[project_id] AND cin.[instance_id] = ssajh.[instance_id]
																								LEFT JOIN [report].[htmlSkipRules] rsr ON	rsr.[module] = 'health-check'
																																			AND rsr.[rule_id] = 16
																																			AND rsr.[active] = 1
																																			AND (rsr.[skip_value]=ssajh.[instance_name])
																																			AND (	rsr.[skip_value2] IS NULL
																																				 OR rsr.[skip_value2] = ssajh.[job_name]
																																				)
																								
																								WHERE	(cin.[project_id]=@projectID OR (@flgOptions & 268435456 = 268435456))
																										AND cin.[instance_name] LIKE @sqlServerNameFilter
																										AND cin.[instance_active]=1
																										AND ssajh.[last_execution_status] IN (0, 2, 3) /* 0 = Failed; 2 = Retry; 3 = Canceled */
																										AND (   (ssajh.[last_execution_utc] IS NOT NULL AND ssajh.[last_execution_utc] >= @dateTimeLowerLimitUTC)
																											 OR (ssajh.[last_execution_utc] IS NULL     AND CONVERT([datetime], ssajh.[last_execution_date] + ' ' + ssajh.[last_execution_time], 120) >= @dateTimeLowerLimit)
																											)
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
	ELSE
		SET @HTMLReport = REPLACE(@HTMLReport, '{SQLServerAgentJobsStatusIssuesDetectedCount}', '(N/A)')


	-----------------------------------------------------------------------------------------------------
	-- Long Running SQL Agent Jobs
	-----------------------------------------------------------------------------------------------------
	IF (@flgActions & 4 = 4) AND (@flgOptions & 33554432 = 33554432)
		begin
			SET @ErrMessage = 'Build Report: Long Running SQL Agent Jobs - Issues Detected'
			EXEC [dbo].[usp_logPrintMessage] @customMessage = @ErrMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0
			
			SET @HTMLReportArea=N''
			SET @HTMLReportArea = @HTMLReportArea + 
							N'<A NAME="LongRunningSQLAgentJobsIssuesDetected" class="category-style">Long Running SQL Agent Jobs - Issues Detected</A><br>
							<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="0px" class="no-border">
							<TR VALIGN=TOP>
								<TD class="small-size" COLLSPAN="6">jobs currently running for more than ' + CAST(@reportOptionMaxJobRunningTimeInHours AS [nvarchar]) + N'hours</TD>
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
			
			DECLARE crsLongRunningSQLAgentJobsIssuesDetected CURSOR LOCAL FAST_FORWARD  FOR	SELECT	DISTINCT
																									  ssajh.[instance_name], ssajh.[job_name]
																									, ssajh.[last_execution_date] AS [start_date], ssajh.[last_execution_time] AS [start_time]
																									, [dbo].[ufn_reportHTMLFormatTimeValue](CAST(ssajh.[running_time_sec]*1000 AS [bigint])) AS [running_time]
																									, ssajh.[message]
																							FROM [health-check].[vw_statsSQLAgentJobsHistory] ssajh
																							INNER JOIN [dbo].[vw_catalogInstanceNames] cin ON cin.[project_id] = ssajh.[project_id] AND cin.[instance_id] = ssajh.[instance_id]
																							LEFT JOIN [report].[htmlSkipRules] rsr ON	rsr.[module] = 'health-check'
																																		AND rsr.[rule_id] = 33554432
																																		AND rsr.[active] = 1
																																		AND (    rsr.[skip_value]=ssajh.[instance_name]
																																			 AND ISNULL(rsr.[skip_value2], '') = ISNULL(ssajh.[job_name], '') 
																																			)
																							WHERE	(cin.[project_id]=@projectID OR (@flgOptions & 268435456 = 268435456))
																									AND cin.[instance_name] LIKE @sqlServerNameFilter
																									AND cin.[instance_active]=1
																									AND ssajh.[last_execution_status] = 4
																									AND ssajh.[last_execution_date] IS NOT NULL
																									AND ssajh.[last_execution_time] IS NOT NULL
																									AND (ssajh.[running_time_sec]/3600) >= @reportOptionMaxJobRunningTimeInHours
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
	ELSE
		SET @HTMLReport = REPLACE(@HTMLReport, '{LongRunningSQLAgentJobsIssuesDetectedCount}', '(N/A)')	


	-----------------------------------------------------------------------------------------------------
	--Frequently Fragmented Indexes
	-----------------------------------------------------------------------------------------------------
	IF (@flgActions & 2 = 2) AND (@flgOptions & 16777216 = 16777216)
		begin
			SET @ErrMessage = 'Frequently Fragmented Indexes - Issues Detected'
			EXEC [dbo].[usp_logPrintMessage] @customMessage = @ErrMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0

			DECLARE @indexAnalyzedCount						[int],
					@indexesPerInstance						[int],
					@minimumIndexMaintenanceFrequencyDays	[smallint],
					@analyzeOnlyMessagesFromTheLastHours	[smallint],
					@minimumIndexSizeInPages				[int],
					@minimumIndexFillFactor					[int]

			SET @minimumIndexMaintenanceFrequencyDays = 2
			SET @analyzeOnlyMessagesFromTheLastHours = 24
		
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
			SELECT	@minimumIndexSizeInPages = [value]
			FROM	[report].[htmlOptions]
			WHERE	[name] = N'Minimum Index Size (pages)'
					AND [module] = 'health-check'

			SET @minimumIndexSizeInPages = ISNULL(@minimumIndexSizeInPages, 50000)
		
			-----------------------------------------------------------------------------------------------------
			SELECT	@minimumIndexFillFactor = [value]
			FROM	[report].[htmlOptions]
			WHERE	[name] = N'Minimum Index fill-factor'
					AND [module] = 'health-check'

			SET @minimumIndexFillFactor = ISNULL(@minimumIndexFillFactor, 90)
		
			-----------------------------------------------------------------------------------------------------
			SET @HTMLReportArea=N''
			SET @HTMLReportArea = @HTMLReportArea + 
							N'<A NAME="FrequentlyFragmentedIndexesIssuesDetected" class="category-style">Frequently Fragmented Indexes</A><br>
							<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="0px" class="no-border">
							<TR VALIGN=TOP>
								<TD class="small-size" COLLSPAN="11">indexes which got fragmented in the last ' + CAST(@minimumIndexMaintenanceFrequencyDays AS [nvarchar](32)) + N' day(s), were analyzed in the last ' + CAST(@analyzeOnlyMessagesFromTheLastHours AS [nvarchar](32)) + N' hours, having minimum ' + CAST(@minimumIndexSizeInPages AS [nvarchar]) + N' pages and fillfactor &ge; ' + CAST(@minimumIndexFillFactor AS [nvarchar]) + N'</TD>
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
			SET @ErrMessage = 'analyzing fragmentation logs'
			EXEC [dbo].[usp_logPrintMessage] @customMessage = @ErrMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 1, @messageTreelevel = 1, @stopExecution=0

			IF OBJECT_ID('tempdb..#filteredStatsIndexesFrequentlyFragmented]') IS NOT NULL
				DROP TABLE #filteredStatsIndexesFrequentlyFragmented

			DECLARE @projectToAnalyze [varchar](32)
			SET @projectToAnalyze = CASE WHEN @flgOptions & 268435456 = 268435456 THEN NULL ELSE @projectCode END
			
			SELECT iff.*
			INTO #filteredStatsIndexesFrequentlyFragmented
			FROM 
				(
					SELECT *
					FROM [dbo].[ufn_hcGetIndexesFrequentlyFragmented](@projectToAnalyze, @minimumIndexMaintenanceFrequencyDays, @analyzeOnlyMessagesFromTheLastHours, 'REBUILD') a
					UNION ALL
					SELECT *
					FROM [dbo].[ufn_hcGetIndexesFrequentlyFragmented](@projectToAnalyze, @minimumIndexMaintenanceFrequencyDays, @analyzeOnlyMessagesFromTheLastHours, 'REORGANIZE')b
				)iff
			INNER JOIN [dbo].[vw_catalogInstanceNames]  cin ON iff.[instance_name] = cin.[instance_name]
			LEFT JOIN [report].[htmlSkipRules] rsr ON	rsr.[module] = 'health-check'
														AND rsr.[rule_id] = 16777216
														AND rsr.[active] = 1
														AND (rsr.[skip_value]=iff.[instance_name])
			WHERE cin.[instance_active] = 1
				 AND (cin.[project_id]=@projectID OR (@flgOptions & 268435456 = 268435456))
				 AND cin.[instance_name] LIKE @sqlServerNameFilter
				 AND iff.[page_count] >= @minimumIndexSizeInPages
				 AND iff.[fill_factor] >= @minimumIndexFillFactor

			CREATE INDEX IX_filteredStatsIndexesFrequentlyFragmented_InstanceName ON #filteredStatsIndexesFrequentlyFragmented([instance_name])

			SET @ErrMessage = 'done'
			EXEC [dbo].[usp_logPrintMessage] @customMessage = @ErrMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 1, @messageTreelevel = 1, @stopExecution=0

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
	ELSE
		SET @HTMLReport = REPLACE(@HTMLReport, '{FrequentlyFragmentedIndexesIssuesDetectedCount}', '(N/A)')				
	

	-----------------------------------------------------------------------------------------------------
	--Failed Login Attempts - Issues Detected
	-----------------------------------------------------------------------------------------------------
	IF (@flgActions & 16 = 16) AND (@flgOptions & 536870912 = 536870912)
		begin
			SET @ErrMessage = 'Build Report: Failed Login Attempts - Issues Detected'
			EXEC [dbo].[usp_logPrintMessage] @customMessage = @ErrMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0

			SET @HTMLReportArea=N''
			SET @HTMLReportArea = @HTMLReportArea + 
							N'<A NAME="FailedLoginsAttemptsIssuesDetected" class="category-style">Failed Login Attempts - Issues Detected (last ' + CAST(@reportOptionErrorlogMessageLastHours AS [nvarchar]) + N'h)</A><br>
							<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="0px" class="no-border">
							<TR VALIGN=TOP>
								<TD class="small-size" COLLSPAN="7">limit results to have minimum ' + CAST(@reportOptionFailedLoginAttemptsLimit AS [nvarchar](32)) + N' </TD>
							</TR>
							<TR VALIGN=TOP>
								<TD WIDTH="1130px">
									<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="3px" class="with-border">' +
										N'<TR class="color-3">
											<TH WIDTH="200px" class="details-bold" nowrap>Instance Name</TH>
											<TH WIDTH="160px" class="details-bold" nowrap>Login Name</TH>
											<TH WIDTH= "60px" class="details-bold" nowrap>Occurences</TH>
											<TH WIDTH="710px" class="details-bold">Reason</TH>'

			SET @idx=1		

			-----------------------------------------------------------------------------------------------------
			SET @issuesDetectedCount = 0 
			DECLARE crsErrorlogMessagesInstanceName CURSOR LOCAL FAST_FORWARD FOR	SELECT DISTINCT
																							  [instance_name]
																							, COUNT(*) AS [messages_count]
																					FROM #filteredStatsFailedLoginsAttempts
																					WHERE [occurencies] >= @reportOptionFailedLoginAttemptsLimit
																					GROUP BY [instance_name]
																					ORDER BY [instance_name]
			OPEN crsErrorlogMessagesInstanceName
			FETCH NEXT FROM crsErrorlogMessagesInstanceName INTO @instanceName, @messageCount
			WHILE @@FETCH_STATUS=0
				begin
					SET @issuesDetectedCount = @issuesDetectedCount + @messageCount

					SET @HTMLReportArea = @HTMLReportArea + 
								N'<TR VALIGN="TOP" class="' + CASE WHEN @idx & 1 = 1 THEN 'color-2' ELSE 'color-1' END + '">' + 
										N'<TD WIDTH="200px" class="details" ALIGN="LEFT" nowrap ROWSPAN="' + CAST(@messageCount AS [nvarchar](64)) + '"><A NAME="ErrorlogMessagesIssuesDetected' + @instanceName + N'">' + @instanceName + N'</A></TD>' 

					SET @tmpHTMLReport=N''
					SELECT @tmpHTMLReport=((
											SELECT N'<TD WIDTH="160px" class="details" ALIGN="LEFT" nowrap>' + ISNULL(CONVERT([nvarchar](24), [login_name], 121), N'&nbsp;') + N'</TD>' + 
														N'<TD WIDTH="60px" class="details" ALIGN="LEFT">' + ISNULL(CAST([occurencies] AS [nvarchar](max)), N'&nbsp;') + N'</TD>' + 
														N'<TD WIDTH="710px" class="details" ALIGN="LEFT">' + ISNULL([dbo].[ufn_reportHTMLPrepareText]([reason], 0), N'&nbsp;')  + N'</TD>' + 
													N'</TR>' + 
													CASE WHEN [row_count] > [row_no]
														 THEN N'<TR VALIGN="TOP" class="' + CASE WHEN @idx & 1 = 1 THEN 'color-2' ELSE 'color-1' END + '">'
														 ELSE N''
													END

											FROM (
													SELECT	TOP (@messageCount)
															[login_name], [occurencies], 
															[reason],
															ROW_NUMBER() OVER(ORDER BY [occurencies] DESC, [login_name]) [row_no],
															SUM(1) OVER() AS [row_count]
													FROM	#filteredStatsFailedLoginsAttempts													
													WHERE	[instance_name] = @instanceName
												)X
											ORDER BY [occurencies] DESC, [login_name]
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

			SET @HTMLReport = REPLACE(@HTMLReport, '{FailedLoginsAttemptsIssuesDetectedCount}', '(' + CAST((@issuesDetectedCount) AS [nvarchar]) + ')')
		end
	ELSE
		SET @HTMLReport = REPLACE(@HTMLReport, '{FailedLoginsAttemptsIssuesDetectedCount}', '(N/A)')


	-----------------------------------------------------------------------------------------------------
	--Outdated Backup for Databases - Issues Detected
	-----------------------------------------------------------------------------------------------------
	IF (@flgActions & 2 = 2) AND (@flgOptions & 8192 = 8192)
		begin
			SET @ErrMessage = 'Build Report: Outdated Backup for Databases - Issues Detected'
			EXEC [dbo].[usp_logPrintMessage] @customMessage = @ErrMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0
			
			SET @HTMLReportArea=N''
			SET @HTMLReportArea = @HTMLReportArea + 
							N'<A NAME="DatabaseBACKUPAgeIssuesDetected" class="category-style">Outdated Backup for Databases - Issues Detected</A><br>
							<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="0px" class="no-border">
							<TR VALIGN=TOP>
								<TD class="small-size" COLLSPAN="7">backup age (system db) &gt; ' + CAST(@reportOptionSystemDatabaseBACKUPAgeDays AS [nvarchar](32)) + N' OR backup age (user db) &gt; ' + CAST(@reportOptionUserDatabaseBACKUPAgeDays AS [nvarchar](32)) + N'</TD>
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
																							SELECT    DISTINCT
																									  cin.[machine_name], cin.[instance_name]
																									, cin.[is_clustered], cin.[cluster_node_machine_name]
																									, cdn.[database_name]
																									, shcdd.[size_mb]
																									, shcdd.[last_backup_time]
																									, DATEDIFF(dd, shcdd.[last_backup_time], GETDATE()) AS [backup_age_days]
																									, CASE WHEN (    cdn.[database_name] NOT IN ('master', 'model', 'msdb', 'distribution') 
																												AND DATEDIFF(dd, shcdd.[last_backup_time], GETDATE()) > @reportOptionUserDatabaseBACKUPAgeDays
																												)
																												OR (    cdn.[database_name] IN ('master', 'model', 'msdb', 'distribution') 
																													AND DATEDIFF(dd, shcdd.[last_backup_time], GETDATE()) > @reportOptionSystemDatabaseBACKUPAgeDays
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
																																		AND (	rsr.[skip_value2] IS NULL
																																			 OR rsr.[skip_value2] = cdn.[database_name]
																																			)
																							WHERE cin.[instance_active]=1
																									AND cdn.[active]=1
																									AND (cin.[project_id] = @projectID OR (@flgOptions & 268435456 = 268435456))
																									AND cin.[instance_name] LIKE @sqlServerNameFilter
																									AND CHARINDEX(cdn.[state_desc], @reportOptionDatabaseAdmittedState) <> 0
																									AND rsr.[id] IS NULL
																									AND (   (@reportOptionSkipDatabaseSnapshots = 0)
																										 OR (@reportOptionSkipDatabaseSnapshots = 1 AND shcdd.[is_snapshot] = 0)
																										)
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
										N'<TD WIDTH="120px" class="details" ALIGN="LEFT" nowrap>' + CASE WHEN @isClustered=0 THEN @machineName ELSE dbo.ufn_reportHTMLGetClusterNodeNames(@projectID, @instanceName, @flgOptions) END + N'</TD>' + 
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
	ELSE
		SET @HTMLReport = REPLACE(@HTMLReport, '{DatabaseBACKUPAgeIssuesDetectedCount}', '(N/A)')
		

	-----------------------------------------------------------------------------------------------------
	--Outdated DBCC CHECKDB Databases - Issues Detected
	-----------------------------------------------------------------------------------------------------
	IF (@flgActions & 2 = 2) AND (@flgOptions & 16384 = 16384)
		begin
			SET @ErrMessage = 'Build Report: Outdated DBCC CHECKDB Databases - Issues Detected'
			EXEC [dbo].[usp_logPrintMessage] @customMessage = @ErrMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0
			
			SET @HTMLReportArea=N''
			SET @HTMLReportArea = @HTMLReportArea + 
							N'<A NAME="DatabaseDBCCCHECKDBAgeIssuesDetected" class="category-style">Outdated DBCC CHECKDB Databases - Issues Detected</A><br>
							<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="0px" class="no-border">
							<TR VALIGN=TOP>
								<TD class="small-size" COLLSPAN="7">dbcc checkdb age (system db) &gt; ' + CAST(@reportOptionSystemDBCCCHECKDBAgeDays AS [nvarchar](32)) + N' OR dbcc checkdb age (user db) &gt; ' + CAST(@reportOptionUserDBCCCHECKDBAgeDays AS [nvarchar](32)) + N'</TD>
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

			DECLARE crsDatabaseDBCCCHECKDBAgeIssuesDetected CURSOR LOCAL FAST_FORWARD  FOR	SELECT  DISTINCT
																									  cin.[machine_name], cin.[instance_name]
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
																																		AND (	rsr.[skip_value2] IS NULL
																																			 OR rsr.[skip_value2] = cdn.[database_name]
																																			)
																							WHERE cin.[instance_active]=1
																									AND cdn.[active]=1
																									AND (cin.[project_id] = @projectID OR (@flgOptions & 268435456 = 268435456))
																									AND cin.[instance_name] LIKE @sqlServerNameFilter
																									AND (
																											(    cdn.[database_name] NOT IN ('master', 'model', 'msdb', 'distribution') 
																												AND DATEDIFF(dd, shcdd.[last_dbcc checkdb_time], GETDATE()) > @reportOptionUserDBCCCHECKDBAgeDays
																											)
																											OR (    cdn.[database_name] IN ('master', 'model', 'msdb', 'distribution') 
																												AND DATEDIFF(dd, shcdd.[last_dbcc checkdb_time], GETDATE()) > @reportOptionSystemDBCCCHECKDBAgeDays
																											)
																											OR (
																													cdn.[database_name] NOT IN ('tempdb')
																												AND shcdd.[last_dbcc checkdb_time] IS NULL
																											)
																										)
																									AND CHARINDEX(cdn.[state_desc], 'ONLINE')<>0
																									AND cin.[version] NOT LIKE '8.%'
																									AND rsr.[id] IS NULL
																									AND (   (@reportOptionSkipDatabaseSnapshots = 0)
																										 OR (@reportOptionSkipDatabaseSnapshots = 1 AND shcdd.[is_snapshot] = 0)
																										)
																							ORDER BY [instance_name], [machine_name], [dbcc_checkdb_age_days] DESC, [database_name]
			OPEN crsDatabaseDBCCCHECKDBAgeIssuesDetected
			FETCH NEXT FROM crsDatabaseDBCCCHECKDBAgeIssuesDetected INTO @machineName, @instanceName, @isClustered, @clusterNodeName, @databaseName, @dbSize, @lastCheckDBDate, @lastDatabaseEventAgeDays
			WHILE @@FETCH_STATUS=0
				begin
					SET @HTMLReportArea = @HTMLReportArea + 
								N'<TR VALIGN="TOP" class="' + CASE WHEN @idx & 1 = 1 THEN 'color-2' ELSE 'color-1' END + '">' + 
										N'<TD WIDTH="120px" class="details" ALIGN="LEFT" nowrap>' + CASE WHEN @isClustered=0 THEN @machineName ELSE dbo.ufn_reportHTMLGetClusterNodeNames(@projectID, @instanceName, @flgOptions) END + N'</TD>' + 
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
	ELSE
		SET @HTMLReport = REPLACE(@HTMLReport, '{DatabaseDBCCCHECKDBAgeIssuesDetectedCount}', '(N/A)')


	-----------------------------------------------------------------------------------------------------
	--Databases with Auto Close / Shrink - Issues Detected
	-----------------------------------------------------------------------------------------------------
	IF (@flgActions & 2 = 2) AND (@flgOptions & 512 = 512)
		begin
			SET @ErrMessage = 'Build Report: Databases with Auto Close / Shrink - Issues Detected'
			EXEC [dbo].[usp_logPrintMessage] @customMessage = @ErrMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0
			
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

			DECLARE crsDatabasesStatusIssuesDetected CURSOR LOCAL FAST_FORWARD  FOR	SELECT  DISTINCT
																							  cin.[machine_name], cin.[instance_name]
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
																																AND (	rsr.[skip_value2] IS NULL
																																		OR rsr.[skip_value2] = cdn.[database_name]
																																	)
																					WHERE cin.[instance_active]=1
																							AND cdn.[active]=1
																							AND (cin.[project_id] = @projectID OR (@flgOptions & 268435456 = 268435456))
																							AND cin.[instance_name] LIKE @sqlServerNameFilter
																							AND (shcdd.[is_auto_close]=1 OR shcdd.[is_auto_shrink]=1)
																							AND rsr.[id] IS NULL
																					ORDER BY cin.[instance_name], cin.[machine_name], cdn.[database_name]
			OPEN crsDatabasesStatusIssuesDetected
			FETCH NEXT FROM crsDatabasesStatusIssuesDetected INTO @machineName, @instanceName, @isClustered, @clusterNodeName, @databaseName, @isAutoClose, @isAutoShrink
			WHILE @@FETCH_STATUS=0
				begin
					SET @HTMLReportArea = @HTMLReportArea + 
								N'<TR VALIGN="TOP" class="' + CASE WHEN @idx & 1 = 1 THEN 'color-2' ELSE 'color-1' END + '">' + 
										N'<TD WIDTH="120px" class="details" ALIGN="LEFT" nowrap>' + CASE WHEN @isClustered=0 THEN @machineName ELSE dbo.ufn_reportHTMLGetClusterNodeNames(@projectID, @instanceName, @flgOptions) END + N'</TD>' + 
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
	ELSE
		SET @HTMLReport = REPLACE(@HTMLReport, '{DatabasesWithAutoCloseShrinkIssuesDetectedCount}', '(N/A)')	
	

	-----------------------------------------------------------------------------------------------------
	--Databases with Improper Page Verify Option
	-----------------------------------------------------------------------------------------------------
	IF (@flgActions & 2 = 2) AND (@flgOptions & 8388608 = 8388608)
		begin
			SET @ErrMessage = 'Databases with Improper Page Verify Option - Issues Detected'
			EXEC [dbo].[usp_logPrintMessage] @customMessage = @ErrMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0
		
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

			DECLARE crsDatabasePageVerifyIssuesDetected CURSOR LOCAL FAST_FORWARD FOR	SELECT  DISTINCT
																								  cin.[machine_name], cin.[instance_name]
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
																																	AND (	rsr.[skip_value2] IS NULL
																																			OR rsr.[skip_value2] = cdn.[database_name]
																																		)
																						WHERE cin.[instance_active]=1
																								AND cdn.[active]=1
																								AND (cin.[project_id] = @projectID OR (@flgOptions & 268435456 = 268435456))
																								AND cin.[instance_name] LIKE @sqlServerNameFilter
																								AND cdn.[database_name] NOT IN ('tempdb')
																								AND (   
																										(     shcdd.[page_verify_option_desc] <> 'CHECKSUM'
																										  AND cin.[version] NOT LIKE '8.%'
																										)
																									 OR (     shcdd.[page_verify_option_desc] = 'NONE'
																										  AND cin.[version] LIKE '8.%'
																										)
																									)
																								AND CHARINDEX(cdn.[state_desc], @reportOptionDatabaseAdmittedState)<>0
																								AND rsr.[id] IS NULL
																						ORDER BY cin.[instance_name], cin.[machine_name], cdn.[database_name]
			OPEN crsDatabasePageVerifyIssuesDetected
			FETCH NEXT FROM crsDatabasePageVerifyIssuesDetected INTO @machineName, @instanceName, @isClustered, @clusterNodeName, @databaseName, @version, @pageVerify, @compatibilityLevel
			WHILE @@FETCH_STATUS=0
				begin
					SET @HTMLReportArea = @HTMLReportArea + 
								N'<TR VALIGN="TOP" class="' + CASE WHEN @idx & 1 = 1 THEN 'color-2' ELSE 'color-1' END + '">' + 
										N'<TD WIDTH="120px" class="details" ALIGN="LEFT" nowrap>' + CASE WHEN @isClustered=0 THEN @machineName ELSE dbo.ufn_reportHTMLGetClusterNodeNames(@projectID, @instanceName, @flgOptions) END + N'</TD>' + 
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
	ELSE
		SET @HTMLReport = REPLACE(@HTMLReport, '{DatabasePageVerifyIssuesDetectedCount}', '(N/A)')


	-----------------------------------------------------------------------------------------------------
	--Databases with Fixed File(s) Size - Issues Detected
	-----------------------------------------------------------------------------------------------------
	IF (@flgActions & 2 = 2) AND (@flgOptions & 4194304 = 4194304)
		begin
			SET @ErrMessage = 'Databases with Fixed File(s) Size - Issues Detected'
			EXEC [dbo].[usp_logPrintMessage] @customMessage = @ErrMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0
		
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
																				SELECT DISTINCT
																						  cin.[instance_name]
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
																															AND (	rsr.[skip_value2] IS NULL
																																	OR rsr.[skip_value2] = cdn.[database_name]
																																)
																				WHERE	cin.[instance_active]=1
																						AND cdn.[active]=1
																						AND (cin.[project_id] = @projectID OR (@flgOptions & 268435456 = 268435456))
																						AND cin.[instance_name] LIKE @sqlServerNameFilter
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
	ELSE
		SET @HTMLReport = REPLACE(@HTMLReport, '{DatabaseFixedFileSizeIssuesDetectedCount}', '(N/A)')


	-----------------------------------------------------------------------------------------------------
	--Low Free Disk Space - Permission Errors
	-----------------------------------------------------------------------------------------------------
	IF (@flgActions & 8 = 8) AND (@flgOptions & 131072 = 131072)
		begin
			SET @ErrMessage = 'Build Report: Low Free Disk Space - Permission Errors'
			EXEC [dbo].[usp_logPrintMessage] @customMessage = @ErrMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0
			
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
																									AND (cin.[project_id]=@projectID OR (@flgOptions & 268435456 = 268435456))
																									AND cin.[instance_name] LIKE @sqlServerNameFilter
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
										N'<TD WIDTH="120px" class="details" ALIGN="LEFT" nowrap ROWSPAN="' + CAST(@messageCount AS [nvarchar](64)) + N'"><A NAME="DiskSpaceInformationPermissionErrors' + @instanceName + N'">' + CASE WHEN @isClustered=0 THEN @machineName ELSE dbo.ufn_reportHTMLGetClusterNodeNames(@projectID, @instanceName, @flgOptions) END + N'</A></TD>' + 
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
																	AND (cin.[project_id]=@projectID OR (@flgOptions & 268435456 = 268435456))
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
					AND (cin.[project_id]=@projectID OR (@flgOptions & 268435456 = 268435456))
					AND cin.[instance_name] LIKE @sqlServerNameFilter
					AND lsam.descriptor IN (N'dbo.usp_hcCollectDiskSpaceUsage')

			SET @HTMLReport = REPLACE(@HTMLReport, '{DiskSpaceInformationPermissionErrorsCount}', '(' + CAST((@idx-1) AS [nvarchar]) + ')')
		end
	ELSE
		SET @HTMLReport = REPLACE(@HTMLReport, '{DiskSpaceInformationPermissionErrorsCount}', '(N/A)')


	-----------------------------------------------------------------------------------------------------
	--Low Free Disk Space - Issues Detected
	-----------------------------------------------------------------------------------------------------
	IF (@flgActions & 8 = 8) AND (@flgOptions & 262144 = 262144)
		begin
			SET @ErrMessage = 'Build Report: Low Free Disk Space - Issues Detected'
			EXEC [dbo].[usp_logPrintMessage] @customMessage = @ErrMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0
			
			SET @HTMLReportArea=N''
			SET @HTMLReportArea = @HTMLReportArea + 
							N'<A NAME="DiskSpaceInformationIssuesDetected" class="category-style">Low Free Disk Space - Issues Detected</A><br>
							<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="0px" class="no-border">
							<TR VALIGN=TOP>
								<TD class="small-size" COLLSPAN="8">free disk space (%) &lt; ' + CAST(@reportOptionFreeDiskMinPercent  AS [nvarchar](32)) + N' OR free disk space (MB) &lt; ' + CAST(@reportOptionFreeDiskMinSpaceMB AS [nvarchar](32)) + N'</TD>
							</TR>
							<TR VALIGN=TOP>
								<TD WIDTH="1130px">
									<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="3px" class="with-border">' +
										N'<TR class="color-3">
											<TH WIDTH="120px" class="details-bold" nowrap>Machine Name</TH>
											<TH WIDTH="200px" class="details-bold" nowrap>Instance Name</TH>
											<TH WIDTH="120px" class="details-bold" nowrap>Clustered</TH>
											<TH WIDTH="100px" class="details-bold">Logical Drive</TH>
											<TH WIDTH="230px" class="details-bold" nowrap>Volume Mount Point</TH>
											<TH WIDTH="120px" class="details-bold">Total Size (GB)</TH>
											<TH WIDTH="120px" class="details-bold" wrap>Available Space (GB)</TH>
											<TH WIDTH="120px" class="details-bold" wrap>Percent Available (%)</TH>'

			SET @idx=1		

			DECLARE crsDiskSpaceInformationIssuesDetected CURSOR LOCAL FAST_FORWARD  FOR	SELECT    cin.[machine_name], cin.[instance_name]
																									, cin.[is_clustered], cin.[cluster_node_machine_name]
																									, dsi.[logical_drive]
																									, dsi.[volume_mount_point]
																									, CAST(ROUND(MAX(dsi.[total_size_mb])/1024, 0) AS [int]) AS [total_size_gb]
																									, CAST(ROUND(MIN(dsi.[available_space_mb])/1024, 0) AS [int]) [available_space_gb]
																									, MIN(dsi.[percent_available]) AS [percent_available]
																							FROM [dbo].[vw_catalogInstanceNames]  cin
																							INNER JOIN [health-check].[vw_statsDiskSpaceInfo]		dsi	ON dsi.[project_id] = cin.[project_id] AND dsi.[instance_id] = cin.[instance_id]
																							LEFT  JOIN 
																										(
																											SELECT DISTINCT [project_id], [instance_id], [volume_mount_point] 
																											FROM [health-check].[vw_statsDatabaseDetails]
																										)   cdd ON cdd.[project_id] = cin.[project_id] AND cdd.[instance_id] = cin.[instance_id]
																							LEFT JOIN [report].[htmlSkipRules] rsr ON	rsr.[module] = 'health-check'
																																		AND rsr.[rule_id] = 262144
																																		AND rsr.[active] = 1
																																		AND (rsr.[skip_value] = cin.[machine_name] OR rsr.[skip_value]=cin.[instance_name])
																																		AND (	rsr.[skip_value2] IS NULL
																																			 OR rsr.[skip_value2] = dsi.[volume_mount_point]
																																			)
																							WHERE cin.[instance_active]=1
																									AND (cin.[project_id]=@projectID OR (@flgOptions & 268435456 = 268435456))
																									AND cin.[instance_name] LIKE @sqlServerNameFilter
																									AND (    (	  dsi.[percent_available] IS NOT NULL 
																												AND dsi.[percent_available] < @reportOptionFreeDiskMinPercent 
																												)
																											OR 
																											(	   dsi.[percent_available] IS NULL 
																												AND dsi.[available_space_mb] IS NOT NULL 
																												AND dsi.[available_space_mb] < @reportOptionFreeDiskMinSpaceMB
																											)
																										)
																									AND (dsi.[logical_drive] IN ('C') OR CHARINDEX(dsi.[logical_drive], cdd.[volume_mount_point])>0)
																									AND rsr.[id] IS NULL
																							GROUP BY  cin.[machine_name], cin.[instance_name]
																									, cin.[is_clustered], cin.[cluster_node_machine_name]
																									, dsi.[logical_drive]
																									, dsi.[volume_mount_point]
																							ORDER BY cin.[instance_name], cin.[machine_name]
			OPEN crsDiskSpaceInformationIssuesDetected
			FETCH NEXT FROM crsDiskSpaceInformationIssuesDetected INTO @machineName, @instanceName, @isClustered, @clusterNodeName, @logicalDrive, @volumeMountPoint, @diskTotalSizeGB, @diskAvailableSpaceGB, @diskPercentAvailable
			WHILE @@FETCH_STATUS=0
				begin
					SET @HTMLReportArea = @HTMLReportArea + 
								N'<TR VALIGN="TOP" class="' + CASE WHEN @idx & 1 = 1 THEN 'color-2' ELSE 'color-1' END + '">' + 
										N'<TD WIDTH="120px" class="details" ALIGN="LEFT" nowrap>' + CASE WHEN @isClustered=0 THEN @machineName ELSE dbo.ufn_reportHTMLGetClusterNodeNames(@projectID, @instanceName, @flgOptions) END + N'</TD>' + 
										N'<TD WIDTH="200px" class="details" ALIGN="LEFT" nowrap>' + @instanceName + N'</TD>' + 
										N'<TD WIDTH="120px" class="details" ALIGN="CENTER" nowrap>' + CASE WHEN @isClustered=0 THEN N'No' ELSE N'Yes' + ISNULL(N'<BR>[' + @clusterNodeName + ']', N'&nbsp;') END + N'</TD>' + 
										N'<TD WIDTH="100px" class="details" ALIGN="CENTER" nowrap>' + ISNULL(@logicalDrive, N'&nbsp;') + N'</TD>' + 
										N'<TD WIDTH="230px" class="details" ALIGN="LEFT" nowrap>' + ISNULL(@volumeMountPoint, N'&nbsp;') + N'</TD>' + 
										N'<TD WIDTH="120px" class="details" ALIGN="RIGHT" nowrap>' + ISNULL(CAST(@diskTotalSizeGB AS [nvarchar](64)), N'&nbsp;')+ N'</TD>' + 
										N'<TD WIDTH="120px" class="details" ALIGN="RIGHT" nowrap>' + ISNULL(CAST(@diskAvailableSpaceGB AS [nvarchar](64)), N'&nbsp;')+ N'</TD>' + 
										N'<TD WIDTH="120px" class="details" ALIGN="RIGHT" nowrap>' + ISNULL(CAST(@diskPercentAvailable AS [nvarchar](64)), N'&nbsp;')+ N'</TD>' + 
									N'</TR>'
					SET @idx=@idx+1

					FETCH NEXT FROM crsDiskSpaceInformationIssuesDetected INTO @machineName, @instanceName, @isClustered, @clusterNodeName, @logicalDrive, @volumeMountPoint, @diskTotalSizeGB, @diskAvailableSpaceGB, @diskPercentAvailable
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
	ELSE
		SET @HTMLReport = REPLACE(@HTMLReport, '{DiskSpaceInformationIssuesDetectedCount}', '(N/A)')
	

	-----------------------------------------------------------------------------------------------------
	--System Databases Size - Issues Detected
	-----------------------------------------------------------------------------------------------------
	IF (@flgActions & 2 = 2) AND (@flgOptions & 128 = 128)
		begin
			SET @ErrMessage = 'Build Report: System Databases Size - Issues Detected'
			EXEC [dbo].[usp_logPrintMessage] @customMessage = @ErrMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0
			
			SET @HTMLReportArea=N''
			SET @HTMLReportArea = @HTMLReportArea + 
							N'<A NAME="SystemDatabasesSizeIssuesDetected" class="category-style">System Databases Size - Issues Detected</A><br>
							<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="0px" class="no-border">
							<TR VALIGN=TOP>
								<TD class="small-size" COLLSPAN="5">size master (MB) &ge; ' + CAST(@reportOptionDatabaseMaxSizeMaster AS [nvarchar](32)) + N' OR size msdb (MB) &ge; ' + CAST(@reportOptionDatabaseMaxSizeMSDB AS [nvarchar](32)) + N'</TD>
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

			DECLARE crsDatabasesStatusIssuesDetected CURSOR LOCAL FAST_FORWARD  FOR	SELECT  DISTINCT
																							  cin.[machine_name], cin.[instance_name]
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
																																AND (	rsr.[skip_value2] IS NULL
																																		OR rsr.[skip_value2] = cdn.[database_name]
																																	)
																					WHERE cin.[instance_active]=1
																							AND cdn.[active]=1
																							AND (cin.[project_id] = @projectID OR (@flgOptions & 268435456 = 268435456))
																							AND cin.[instance_name] LIKE @sqlServerNameFilter
																							AND (   (cdn.[database_name]='master' AND shcdd.[size_mb] >= @reportOptionDatabaseMaxSizeMaster AND @reportOptionDatabaseMaxSizeMaster<>0)
																								 OR (cdn.[database_name]='msdb'   AND shcdd.[size_mb] >= @reportOptionDatabaseMaxSizeMSDB   AND @reportOptionDatabaseMaxSizeMSDB<>0)
																								)
																							AND rsr.[id] IS NULL
																					ORDER BY shcdd.[size_mb] DESC, cin.[instance_name], cin.[machine_name], cdn.[database_name]
			OPEN crsDatabasesStatusIssuesDetected
			FETCH NEXT FROM crsDatabasesStatusIssuesDetected INTO @machineName, @instanceName, @isClustered, @clusterNodeName, @databaseName, @dbSize
			WHILE @@FETCH_STATUS=0
				begin
					SET @HTMLReportArea = @HTMLReportArea + 
								N'<TR VALIGN="TOP" class="' + CASE WHEN @idx & 1 = 1 THEN 'color-2' ELSE 'color-1' END + '">' + 
										N'<TD WIDTH="120px" class="details" ALIGN="LEFT" nowrap>' + CASE WHEN @isClustered=0 THEN @machineName ELSE dbo.ufn_reportHTMLGetClusterNodeNames(@projectID, @instanceName, @flgOptions) END + N'</TD>' + 
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
	ELSE
		SET @HTMLReport = REPLACE(@HTMLReport, '{SystemDatabasesSizeIssuesDetectedCount}', '(N/A)')	
		

	-----------------------------------------------------------------------------------------------------
	--Big Size for Database Log files - Issues Detected
	-----------------------------------------------------------------------------------------------------
	IF (@flgActions & 2 = 2) AND (@flgOptions & 1024 = 1024)
		begin
			SET @ErrMessage = 'Build Report: Big Size for Database Log files - Issues Detected'
			EXEC [dbo].[usp_logPrintMessage] @customMessage = @ErrMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0
			
			SET @HTMLReportArea=N''
			SET @HTMLReportArea = @HTMLReportArea + 
							N'<A NAME="DatabaseMaxLogSizeIssuesDetected" class="category-style">Big Size for Database Log files - Issues Detected</A><br>
							<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="0px" class="no-border">
							<TR VALIGN=TOP>
								<TD class="small-size" COLLSPAN="6">log size (MB) &ge; ' + CAST(@reportOptionLogMaxSize AS [nvarchar](32)) + N'</TD>
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
											<TH WIDTH="80px" class="details-bold" nowrap>Log Used (%)</TH>
											<TH WIDTH="80px" class="details-bold" nowrap>Reclaimable Space (MB)</TH>											
											'

			SET @idx=1		

			DECLARE crsDatabaseMaxLogSizeIssuesDetected CURSOR LOCAL FAST_FORWARD  FOR	SELECT  DISTINCT
																								  cin.[machine_name], cin.[instance_name]
																								, cin.[is_clustered], cin.[cluster_node_machine_name]
																								, cdn.[database_name]
																								, shcdd.[size_mb]
																								, shcdd.[log_size_mb]
																								, shcdd.[log_space_used_percent]
																								, ((100.0 - shcdd.[log_space_used_percent]) * shcdd.[log_size_mb]) / 100 AS [reclaimable_space_mb]
																						FROM [dbo].[vw_catalogInstanceNames]  cin
																						INNER JOIN [dbo].[vw_catalogDatabaseNames]			  cdn	ON cdn.[project_id] = cin.[project_id] AND cdn.[instance_id] = cin.[instance_id]
																						INNER  JOIN [health-check].[vw_statsDatabaseDetails] shcdd ON shcdd.[catalog_database_id] = cdn.[catalog_database_id] AND shcdd.[instance_id] = cdn.[instance_id]
																						LEFT JOIN [report].[htmlSkipRules] rsr ON	rsr.[module] = 'health-check'
																																	AND rsr.[rule_id] = 1024
																																	AND rsr.[active] = 1
																																	AND (rsr.[skip_value] = cin.[machine_name] OR rsr.[skip_value]=cin.[instance_name])
																																	AND (	rsr.[skip_value2] IS NULL
																																			OR rsr.[skip_value2] = cdn.[database_name]
																																		)
																						WHERE cin.[instance_active]=1
																								AND cdn.[active]=1
																								AND (cin.[project_id] = @projectID OR (@flgOptions & 268435456 = 268435456))
																								AND cin.[instance_name] LIKE @sqlServerNameFilter
																								AND shcdd.[log_size_mb] >= @reportOptionLogMaxSize 
																								AND rsr.[id] IS NULL
																						ORDER BY cin.[instance_name], cin.[machine_name], [reclaimable_space_mb] DESC, cdn.[database_name]
			OPEN crsDatabaseMaxLogSizeIssuesDetected
			FETCH NEXT FROM crsDatabaseMaxLogSizeIssuesDetected INTO @machineName, @instanceName, @isClustered, @clusterNodeName, @databaseName, @dbSize, @logSizeMB, @logSpaceUsedPercent, @reclaimableSpaceMB
			WHILE @@FETCH_STATUS=0
				begin
					SET @HTMLReportArea = @HTMLReportArea + 
								N'<TR VALIGN="TOP" class="' + CASE WHEN @idx & 1 = 1 THEN 'color-2' ELSE 'color-1' END + '">' + 
										N'<TD WIDTH="120px" class="details" ALIGN="LEFT" nowrap>' + CASE WHEN @isClustered=0 THEN @machineName ELSE dbo.ufn_reportHTMLGetClusterNodeNames(@projectID, @instanceName, @flgOptions) END + N'</TD>' + 
										N'<TD WIDTH="200px" class="details" ALIGN="LEFT" nowrap>' + @instanceName + N'</TD>' + 
										N'<TD WIDTH="120px" class="details" ALIGN="CENTER" nowrap>' + CASE WHEN @isClustered=0 THEN N'No' ELSE N'Yes' + ISNULL(N'<BR>[' + @clusterNodeName + ']', N'&nbsp;') END + N'</TD>' + 
										N'<TD WIDTH="370px" class="details" ALIGN="LEFT">' + ISNULL(@databaseName, N'&nbsp;') + N'</TD>' + 
										N'<TD WIDTH="80px" class="details" ALIGN="RIGHT" nowrap>' + ISNULL(CAST(@dbSize AS [nvarchar](64)), N'&nbsp;')+ N'</TD>' + 
										N'<TD WIDTH="80px" class="details" ALIGN="RIGHT" nowrap>' + ISNULL(CAST(@logSizeMB AS [nvarchar](64)), N'&nbsp;')+ N'</TD>' + 
										N'<TD WIDTH="80px" class="details" ALIGN="RIGHT" nowrap>' + ISNULL(CAST(@logSpaceUsedPercent AS [nvarchar](64)), N'&nbsp;')+ N'</TD>' + 
										N'<TD WIDTH="80px" class="details" ALIGN="RIGHT" nowrap>' + ISNULL(CAST(@reclaimableSpaceMB AS [nvarchar](64)), N'&nbsp;')+ N'</TD>' + 

									N'</TR>'
					SET @idx=@idx+1

					FETCH NEXT FROM crsDatabaseMaxLogSizeIssuesDetected INTO @machineName, @instanceName, @isClustered, @clusterNodeName, @databaseName, @dbSize, @logSizeMB, @logSpaceUsedPercent, @reclaimableSpaceMB
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
	ELSE
		SET @HTMLReport = REPLACE(@HTMLReport, '{DatabaseMaxLogSizeIssuesDetectedCount}', '(N/A)')


	-----------------------------------------------------------------------------------------------------
	--Low Usage of Data Space - Issues Detected
	-----------------------------------------------------------------------------------------------------
	IF (@flgActions & 2 = 2) AND (@flgOptions & 2048 = 2048)
		begin
			SET @ErrMessage = 'Build Report: Low Usage of Data Space - Issues Detected'
			EXEC [dbo].[usp_logPrintMessage] @customMessage = @ErrMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0
			
			SET @HTMLReportArea=N''
			SET @HTMLReportArea = @HTMLReportArea + 
							N'<A NAME="DatabaseMinDataSpaceIssuesDetected" class="category-style">Low Usage of Data Space - Issues Detected</A><br>
							<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="0px" class="no-border">
							<TR VALIGN=TOP>
								<TD class="small-size" COLLSPAN="8">size (MB) &ge; ' + CAST(@reportOptionDBMinSizeForAnalysis  AS [nvarchar](32)) + N' AND data size used (%) &le; ' + CAST(@reportOptionDataSpaceMinPercent AS [nvarchar](32)) + N' AND data space available (mb) &ge; ' + CAST(@reportOptionMinSpaceToReclaim AS [nvarchar](32)) + N'</TD>
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
					
			DECLARE crsDatabaseMinDataSpaceIssuesDetected CURSOR LOCAL FAST_FORWARD  FOR SELECT  DISTINCT
																								  cin.[machine_name], cin.[instance_name]
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
																																	AND (	rsr.[skip_value2] IS NULL
																																			OR rsr.[skip_value2] = cdn.[database_name]
																																		)
																						WHERE cin.[instance_active]=1
																								AND cdn.[active]=1
																								AND (cin.[project_id] = @projectID OR (@flgOptions & 268435456 = 268435456))
																								AND cin.[instance_name] LIKE @sqlServerNameFilter
																								AND shcdd.[size_mb]>=@reportOptionDBMinSizeForAnalysis 
																								AND shcdd.[data_space_used_percent] <= @reportOptionDataSpaceMinPercent 
																								AND @reportOptionDataSpaceMinPercent<>0
																								AND cdn.[database_name] NOT IN ('master', 'msdb', 'model', 'tempdb', 'distribution')
																								AND rsr.[id] IS NULL
																								AND (((100.0 - shcdd.[data_space_used_percent]) * shcdd.[data_size_mb]) / 100 ) >= @reportOptionMinSpaceToReclaim
																						ORDER BY cin.[instance_name], cin.[machine_name], [reclaimable_space_mb] DESC, cdn.[database_name]
			OPEN crsDatabaseMinDataSpaceIssuesDetected
			FETCH NEXT FROM crsDatabaseMinDataSpaceIssuesDetected INTO @machineName, @instanceName, @isClustered, @clusterNodeName, @databaseName, @dbSize, @dataSizeMB, @dataSpaceUsedPercent, @reclaimableSpaceMB
			WHILE @@FETCH_STATUS=0
				begin
					SET @HTMLReportArea = @HTMLReportArea + 
								N'<TR VALIGN="TOP" class="' + CASE WHEN @idx & 1 = 1 THEN 'color-2' ELSE 'color-1' END + '">' + 
										N'<TD WIDTH="120px" class="details" ALIGN="LEFT" nowrap>' + CASE WHEN @isClustered=0 THEN @machineName ELSE dbo.ufn_reportHTMLGetClusterNodeNames(@projectID, @instanceName, @flgOptions) END + N'</TD>' + 
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
	ELSE
		SET @HTMLReport = REPLACE(@HTMLReport, '{DatabaseMinDataSpaceIssuesDetectedCount}', '(N/A)')
	

	-----------------------------------------------------------------------------------------------------
	--High Usage of Log Space - Issues Detected
	-----------------------------------------------------------------------------------------------------
	IF (@flgActions & 2 = 2) AND (@flgOptions & 32768 = 32768)
		begin
			SET @ErrMessage = 'Build Report: High Usage of Log Space - Issues Detected'
			EXEC [dbo].[usp_logPrintMessage] @customMessage = @ErrMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0
			
			SET @HTMLReportArea=N''
			SET @HTMLReportArea = @HTMLReportArea + 
							N'<A NAME="DatabaseMaxLogSpaceIssuesDetected" class="category-style">High Usage of Log Space - Issues Detected</A><br>
							<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="0px" class="no-border">
							<TR VALIGN=TOP>
								<TD class="small-size" COLLSPAN="8">size (MB) &ge; ' + CAST(@reportOptionDBMinSizeForAnalysis  AS [nvarchar](32)) + N' AND log size used (%) &ge; ' + CAST(@reportOptionLogSpaceMaxPercent AS [nvarchar](32)) + N' AND log space available (mb) &ge; ' + CAST(@reportOptionMinSpaceToReclaim AS [nvarchar](32)) + N'</TD>
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
																																	AND (	rsr.[skip_value2] IS NULL
																																			OR rsr.[skip_value2] = cdn.[database_name]
																																		)
																						WHERE cin.[instance_active]=1
																								AND cdn.[active]=1
																								AND (cin.[project_id] = @projectID OR (@flgOptions & 268435456 = 268435456))
																								AND cin.[instance_name] LIKE @sqlServerNameFilter
																								AND shcdd.[size_mb]>=@reportOptionDBMinSizeForAnalysis 
																								AND shcdd.[log_space_used_percent] >= @reportOptionLogSpaceMaxPercent 
																								AND @reportOptionLogSpaceMaxPercent<>0
																								AND cdn.[database_name] NOT IN ('master', 'msdb', 'model', 'tempdb', 'distribution')
																								AND rsr.[id] IS NULL
																								AND (((100.0 - shcdd.[log_space_used_percent]) * shcdd.[log_size_mb]) / 100) >= @reportOptionMinSpaceToReclaim
																						ORDER BY --[available_space_mb] DESC, 
																								 cin.[instance_name], cin.[machine_name], shcdd.[data_space_used_percent] DESC, cdn.[database_name]
			OPEN crsDatabaseMaxLogSpaceIssuesDetected
			FETCH NEXT FROM crsDatabaseMaxLogSpaceIssuesDetected INTO @machineName, @instanceName, @isClustered, @clusterNodeName, @databaseName, @dbSize, @logSizeMB, @logSpaceUsedPercent, @reclaimableSpaceMB
			WHILE @@FETCH_STATUS=0
				begin
					SET @HTMLReportArea = @HTMLReportArea + 
								N'<TR VALIGN="TOP" class="' + CASE WHEN @idx & 1 = 1 THEN 'color-2' ELSE 'color-1' END + '">' + 
										N'<TD WIDTH="120px" class="details" ALIGN="LEFT" nowrap>' + CASE WHEN @isClustered=0 THEN @machineName ELSE dbo.ufn_reportHTMLGetClusterNodeNames(@projectID, @instanceName, @flgOptions) END + N'</TD>' + 
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
	ELSE
		SET @HTMLReport = REPLACE(@HTMLReport, '{DatabaseMaxLogSpaceIssuesDetectedCount}', '(N/A)')	
	

	-----------------------------------------------------------------------------------------------------
	--Log vs. Data - Allocated Size - Issues Detected
	-----------------------------------------------------------------------------------------------------
	IF (@flgActions & 2 = 2) AND (@flgOptions & 4096 = 4096)
		begin
			SET @ErrMessage = 'Build Report: Log vs. Data - Allocated Size - Issues Detected'
			EXEC [dbo].[usp_logPrintMessage] @customMessage = @ErrMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0
			
			SET @HTMLReportArea=N''
			SET @HTMLReportArea = @HTMLReportArea + 
							N'<A NAME="DatabaseLogVsDataSizeIssuesDetected" class="category-style">Log vs. Data - Allocated Size - Issues Detected</A><br>
							<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="0px" class="no-border">
							<TR VALIGN=TOP>
								<TD class="small-size" COLLSPAN="8">size (MB) &ge; ' + CAST(@reportOptionDBMinSizeForAnalysis  AS [nvarchar](32)) + N' AND log/data size (%) &gt; ' + CAST(@reportOptionLogVsDataPercent AS [nvarchar](32)) + N'</TD>
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
																									SELECT  DISTINCT
																											  cin.[machine_name], cin.[instance_name]
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
																																				AND (	rsr.[skip_value2] IS NULL
																																					 OR rsr.[skip_value2] = cdn.[database_name]
																																					)
																									WHERE cin.[instance_active]=1
																											AND cdn.[active]=1
																											AND (cin.[project_id] = @projectID OR (@flgOptions & 268435456 = 268435456))
																											AND cin.[instance_name] LIKE @sqlServerNameFilter
																											AND shcdd.[data_size_mb] <> 0
																											AND (shcdd.[log_size_mb] / shcdd.[data_size_mb] * 100.) > @reportOptionLogVsDataPercent
																											AND shcdd.[size_mb]>=@reportOptionDBMinSizeForAnalysis 
																											AND cdn.[database_name] NOT IN ('master', 'msdb', 'model', 'tempdb', 'distribution')
																											AND rsr.[id] IS NULL
																								)X
																							WHERE [log_vs_data] >= @reportOptionLogVsDataPercent
																							ORDER BY [instance_name], [machine_name], [log_vs_data] DESC, [database_name]
			OPEN crsDatabaseLogVsDataSizeIssuesDetected
			FETCH NEXT FROM crsDatabaseLogVsDataSizeIssuesDetected INTO @machineName, @instanceName, @isClustered, @clusterNodeName, @databaseName, @dbSize, @dataSizeMB, @logSizeMB, @logVSDataPercent
			WHILE @@FETCH_STATUS=0
				begin
					SET @HTMLReportArea = @HTMLReportArea + 
								N'<TR VALIGN="TOP" class="' + CASE WHEN @idx & 1 = 1 THEN 'color-2' ELSE 'color-1' END + '">' + 
										N'<TD WIDTH="120px" class="details" ALIGN="LEFT" nowrap>' + CASE WHEN @isClustered=0 THEN @machineName ELSE dbo.ufn_reportHTMLGetClusterNodeNames(@projectID, @instanceName, @flgOptions) END + N'</TD>' + 
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
	ELSE
		SET @HTMLReport = REPLACE(@HTMLReport, '{DatabaseLogVsDataSizeIssuesDetectedCount}', '(N/A)')	


	-----------------------------------------------------------------------------------------------------
	--Databases(s) Growth - Issues Detected
	-----------------------------------------------------------------------------------------------------
	IF (@flgActions & 2 = 2) AND (@flgOptions & 1073741824 = 1073741824)
		begin
			DECLARE   @oldDBSize			[numeric](18,3)
					, @oldDataSizeMB		[numeric](18,3)
					, @oldLogSizeMB			[numeric](18,3)
					, @growthSizeMB			[numeric](18,3)
					, @dataGrowthPercent	[numeric](18,3)

			SET @ErrMessage = 'Build Report: Databases(s) Growth - Issues Detected'
			EXEC [dbo].[usp_logPrintMessage] @customMessage = @ErrMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0

			SET @HTMLReportArea=N''
			SET @HTMLReportArea = @HTMLReportArea + 
							N'<A NAME="DatabaseGrowthIssuesDetected" class="category-style">Databases(s) Growth - Issues Detected</A><br>
							<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="0px" class="no-border">
							<TR VALIGN=TOP>
								<TD class="small-size" COLLSPAN="8">database growth within last ' + CAST(@reportOptionGetDBGrowthLastDays AS [nvarchar](32)) + N' days AND (growth size (MB) &ge; ' + CAST(@reportOptionDBGrowthMinSizeMBForAnalysis  AS [nvarchar](32)) + N' OR data growth (%) &ge; ' + CAST(@reportOptionDBGrowthMinPercentForAnalysis AS [nvarchar](32)) + N')</TD>
							</TR>
							<TR VALIGN=TOP>
								<TD WIDTH="1130px">
									<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="3px" class="with-border">' +
										N'<TR class="color-3">
											<TH WIDTH="180px" class="details-bold" nowrap>Instance Name</TH>
											<TH WIDTH="310px" class="details-bold">Database Name</TH>
											<TH WIDTH="80px" class="details-bold" nowrap>Current Size (MB)</TH>
											<TH WIDTH="80px" class="details-bold" nowrap>Old Size (MB)</TH>
											<TH WIDTH="80px" class="details-bold" nowrap>Current Data Size (MB)</TH>
											<TH WIDTH="80px" class="details-bold" nowrap>Old Data Size (MB)</TH>
											<TH WIDTH="80px" class="details-bold" nowrap>Current Log Size (MB)</TH>
											<TH WIDTH="80px" class="details-bold" nowrap>Old Log Size (MB)</TH>
											<TH WIDTH="80px" class="details-bold" nowrap>Growth Size (MB)</TH>
											<TH WIDTH="80px" class="details-bold" nowrap>Data Growth (%)</TH>'

			SET @idx=1		

			DECLARE crsDatabaseLogVsDataSizeIssuesDetected CURSOR LOCAL FAST_FORWARD  FOR	SELECT  DISTINCT
																									  hcdg.[instance_name], hcdg.[database_name], hcdg.[current_size_mb], hcdg.[old_size_mb]
																									, hcdg.[current_data_size_mb], hcdg.[old_data_size_mb], hcdg.[current_log_size_mb], hcdg.[old_log_size_mb]
																									, hcdg.[growth_size_mb], hcdg.[data_growth_percent]
																							FROM #hcReportCapacityDatabaseGrowth		hcdg
																							INNER JOIN [dbo].[vw_catalogInstanceNames]  cin ON	hcdg.[instance_name] = cin.[instance_name] COLLATE DATABASE_DEFAULT
																							INNER JOIN [dbo].[vw_catalogDatabaseNames]	cdn	ON	cdn.[project_id] = cin.[project_id] 
																																				AND cdn.[instance_id] = cin.[instance_id] 
																																				AND cdn.[database_name] = hcdg.[database_name] COLLATE DATABASE_DEFAULT
																							LEFT JOIN [report].[htmlSkipRules] rsr ON	rsr.[module] = 'health-check'
																																		AND rsr.[rule_id] = 1073741824
																																		AND rsr.[active] = 1
																																		AND (rsr.[skip_value] = cin.[machine_name] OR rsr.[skip_value]=cin.[instance_name])
																																		AND (	rsr.[skip_value2] IS NULL
																																			 OR rsr.[skip_value2] = cdn.[database_name]
																																			)
																							WHERE cin.[instance_active]=1
																									AND cdn.[active]=1
																									AND (cin.[project_id] = @projectID OR (@flgOptions & 268435456 = 268435456))
																									AND cin.[instance_name] LIKE @sqlServerNameFilter
																									AND (   hcdg.[data_growth_percent] >= @reportOptionDBGrowthMinPercentForAnalysis
																										 OR hcdg.[growth_size_mb] >= @reportOptionDBGrowthMinSizeMBForAnalysis
																										)
																									AND rsr.[id] IS NULL
																							ORDER BY [instance_name], [growth_size_mb] DESC, [database_name]
			OPEN crsDatabaseLogVsDataSizeIssuesDetected
			FETCH NEXT FROM crsDatabaseLogVsDataSizeIssuesDetected INTO @instanceName, @databaseName, @dbSize, @oldDBSize, @dataSizeMB, @oldDataSizeMB, @logSizeMB, @oldLogSizeMB, @growthSizeMB, @dataGrowthPercent
			WHILE @@FETCH_STATUS=0
				begin
					SET @HTMLReportArea = @HTMLReportArea + 
								N'<TR VALIGN="TOP" class="' + CASE WHEN @idx & 1 = 1 THEN 'color-2' ELSE 'color-1' END + '">' + 
										N'<TD WIDTH="190px" class="details" ALIGN="LEFT" nowrap>' + @instanceName + N'</TD>' + 
										N'<TD WIDTH="310px" class="details" ALIGN="LEFT">' + ISNULL(@databaseName, N'&nbsp;') + N'</TD>' + 
										N'<TD WIDTH="80px" class="details" ALIGN="RIGHT" nowrap>' + ISNULL(CAST(@dbSize AS [nvarchar](64)), N'&nbsp;')+ N'</TD>' + 
										N'<TD WIDTH="80px" class="details" ALIGN="RIGHT" nowrap>' + ISNULL(CAST(@oldDBSize AS [nvarchar](64)), N'&nbsp;')+ N'</TD>' + 
										N'<TD WIDTH="80px" class="details" ALIGN="RIGHT" nowrap>' + ISNULL(CAST(@dataSizeMB AS [nvarchar](64)), N'&nbsp;')+ N'</TD>' + 
										N'<TD WIDTH="80px" class="details" ALIGN="RIGHT" nowrap>' + ISNULL(CAST(@oldDataSizeMB AS [nvarchar](64)), N'&nbsp;')+ N'</TD>' + 
										N'<TD WIDTH="80px" class="details" ALIGN="RIGHT" nowrap>' + ISNULL(CAST(@logSizeMB AS [nvarchar](64)), N'&nbsp;')+ N'</TD>' + 
										N'<TD WIDTH="80px" class="details" ALIGN="RIGHT" nowrap>' + ISNULL(CAST(@oldLogSizeMB AS [nvarchar](64)), N'&nbsp;')+ N'</TD>' + 
										N'<TD WIDTH="80px" class="details" ALIGN="RIGHT" nowrap>' + ISNULL(CAST(@growthSizeMB AS [nvarchar](64)), N'&nbsp;')+ N'</TD>' + 
										N'<TD WIDTH="80px" class="details" ALIGN="RIGHT" nowrap>' + ISNULL(CAST(@dataGrowthPercent AS [nvarchar](64)), N'&nbsp;')+ N'</TD>' + 
									N'</TR>'
					SET @idx=@idx+1

					FETCH NEXT FROM crsDatabaseLogVsDataSizeIssuesDetected INTO @instanceName, @databaseName, @dbSize, @oldDBSize, @dataSizeMB, @oldDataSizeMB, @logSizeMB, @oldLogSizeMB, @growthSizeMB, @dataGrowthPercent
				end

			CLOSE crsDatabaseLogVsDataSizeIssuesDetected
			DEALLOCATE crsDatabaseLogVsDataSizeIssuesDetected

			SET @HTMLReportArea = @HTMLReportArea + N'</TABLE>
								</TD>
							</TR>
						</TABLE>'

			SET @HTMLReportArea = @HTMLReportArea + N'<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="3px"><TR><TD WIDTH="1130px" ALIGN=RIGHT><A HREF="#Home" class="normal">Go Up</A></TD></TR></TABLE>'	
			SET @HTMLReport = @HTMLReport + @HTMLReportArea					

			SET @HTMLReport = REPLACE(@HTMLReport, '{DatabaseGrowthIssuesDetectedCount}', '(' + CAST((@idx-1) AS [nvarchar]) + ')')
		end
	ELSE
		SET @HTMLReport = REPLACE(@HTMLReport, '{DatabaseGrowthIssuesDetectedCount}', '(N/A)')	

	-----------------------------------------------------------------------------------------------------
	--Backup Size Details
	-----------------------------------------------------------------------------------------------------
	IF (@flgActions & 2 = 2)
		begin
			DECLARE   @backupSize			[numeric](18,3)
					, @backupFilesCount		[int]
					, @fullBackupSize		[numeric](18,3)
					, @diffBackupSize		[numeric](18,3)
					, @logBackupSize		[numeric](18,3)

			SET @ErrMessage = 'Build Report: Backup(s) Size Details'
			EXEC [dbo].[usp_logPrintMessage] @customMessage = @ErrMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0

			SET @HTMLReportArea=N''
			SET @HTMLReportArea = @HTMLReportArea + 
							N'<A NAME="BackupSizeDetails" class="category-style">Backup(s) Size Details</A><br>
							<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="0px" class="no-border">
							<TR VALIGN=TOP>
								<TD class="small-size" COLLSPAN="8">database(s) backup size within last ' + CAST(@reportOptionGetBackupSizeLastDays AS [nvarchar](32)) + N' days</TD>
							</TR>
							<TR VALIGN=TOP>
								<TD WIDTH="1130px">
									<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="3px" class="with-border">' +
										N'<TR class="color-3">
											<TH WIDTH="325px" class="details-bold" nowrap>Instance Name</TH>
											<TH WIDTH="115px" class="details-bold" nowrap>Database Count</TH>
											<TH WIDTH="115px" class="details-bold">Database Size (GB)</TH>
											<TH WIDTH="115px" class="details-bold">Backup Size (GB)</TH>
											<TH WIDTH="115px" class="details-bold">Backup Files Count</TH>
											<TH WIDTH="115px" class="details-bold">Full Backup(s) (GB)</TH>
											<TH WIDTH="115px" class="details-bold">Diff Backup(s) (GB)</TH>
											<TH WIDTH="115px" class="details-bold">TLog Backup(s) (GB)</TH>'

			SET @idx=1		

			DECLARE crsDatabaseLogVsDataSizeIssuesDetected CURSOR LOCAL FAST_FORWARD  FOR	SELECT  DISTINCT
																									  hcdb.[instance_name], [database_count], [database_size_gb], [backup_size_gb]
																									, [backup_files_count], [full_backup_gb], [diff_backup_gb], [log_backup_gb]
																							FROM 
																								(
																									SELECT    hcdb.[instance_name]
																											, SUM(hcdb.[database_count])	AS [database_count]
																											, SUM(hcdb.[database_size_gb])	AS [database_size_gb]
																											, SUM(hcdb.[backup_size_gb])	AS [backup_size_gb]
																											, SUM(hcdb.[backup_files_count]) AS [backup_files_count]
																											, SUM(hcdb.[full_backup_gb])	AS [full_backup_gb]
																											, SUM(hcdb.[diff_backup_gb])	AS [diff_backup_gb]
																											, SUM(hcdb.[log_backup_gb])		AS [log_backup_gb]
																									FROM #hcReportCapacityDatabaseBackups		hcdb
																									GROUP BY hcdb.[instance_name]
																								) hcdb
																							INNER JOIN [dbo].[vw_catalogInstanceNames]  cin ON hcdb.[instance_name] = cin.[instance_name] COLLATE DATABASE_DEFAULT
																							LEFT JOIN [report].[htmlSkipRules] rsr ON	rsr.[module] = 'health-check'
																																		AND rsr.[rule_id] = 1073741824
																																		AND rsr.[active] = 1
																																		AND (rsr.[skip_value] = cin.[machine_name] OR rsr.[skip_value]=cin.[instance_name])
																							WHERE cin.[instance_active]=1
																									AND (cin.[project_id] = @projectID OR (@flgOptions & 268435456 = 268435456))
																									AND cin.[instance_name] LIKE @sqlServerNameFilter
																									AND rsr.[id] IS NULL																						
																							ORDER BY [instance_name]
			OPEN crsDatabaseLogVsDataSizeIssuesDetected
			FETCH NEXT FROM crsDatabaseLogVsDataSizeIssuesDetected INTO @instanceName, @dbCount, @dbSize, @backupSize, @backupFilesCount, @fullBackupSize, @diffBackupSize, @logBackupSize
			WHILE @@FETCH_STATUS=0
				begin
					SET @HTMLReportArea = @HTMLReportArea + 
								N'<TR VALIGN="TOP" class="' + CASE WHEN @idx & 1 = 1 THEN 'color-2' ELSE 'color-1' END + '">' + 
										N'<TD WIDTH="325px" class="details" ALIGN="LEFT" nowrap>' + @instanceName + N'</TD>' + 
										N'<TD WIDTH="115px" class="details" ALIGN="RIGHT" nowrap>' + ISNULL(CAST(@dbCount AS [nvarchar](64)), N'&nbsp;')+ N'</TD>' + 
										N'<TD WIDTH="115px" class="details" ALIGN="RIGHT">' + ISNULL(CAST(@dbSize AS [nvarchar](64)), N'&nbsp;')+ N'</TD>' + 
										N'<TD WIDTH="115px" class="details" ALIGN="RIGHT">' + ISNULL(CAST(@backupSize AS [nvarchar](64)), N'&nbsp;')+ N'</TD>' + 
										N'<TD WIDTH="115px" class="details" ALIGN="RIGHT">' + ISNULL(CAST(@backupFilesCount AS [nvarchar](64)), N'&nbsp;')+ N'</TD>' + 
										N'<TD WIDTH="115px" class="details" ALIGN="RIGHT">' + ISNULL(CAST(@fullBackupSize AS [nvarchar](64)), N'&nbsp;')+ N'</TD>' + 
										N'<TD WIDTH="115px" class="details" ALIGN="RIGHT">' + ISNULL(CAST(@diffBackupSize AS [nvarchar](64)), N'&nbsp;')+ N'</TD>' + 
										N'<TD WIDTH="115px" class="details" ALIGN="RIGHT">' + ISNULL(CAST(@logBackupSize AS [nvarchar](64)), N'&nbsp;')+ N'</TD>' + 
									N'</TR>'
					SET @idx=@idx+1

					FETCH NEXT FROM crsDatabaseLogVsDataSizeIssuesDetected INTO @instanceName, @dbCount, @dbSize, @backupSize, @backupFilesCount, @fullBackupSize, @diffBackupSize, @logBackupSize
				end

			CLOSE crsDatabaseLogVsDataSizeIssuesDetected
			DEALLOCATE crsDatabaseLogVsDataSizeIssuesDetected

			SET @HTMLReportArea = @HTMLReportArea + N'</TABLE>
								</TD>
							</TR>
						</TABLE>'

			SET @HTMLReportArea = @HTMLReportArea + N'<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="3px"><TR><TD WIDTH="1130px" ALIGN=RIGHT><A HREF="#Home" class="normal">Go Up</A></TD></TR></TABLE>'	
			SET @HTMLReport = @HTMLReport + @HTMLReportArea					

		end
		
	-----------------------------------------------------------------------------------------------------
	--Errorlog Messages - Permission Errors
	-----------------------------------------------------------------------------------------------------
	IF (@flgActions & 16 = 16) AND (@flgOptions & 524288 = 524288)
		begin
			SET @ErrMessage = 'Build Report: Errorlog Messages - Permission Errors'
			EXEC [dbo].[usp_logPrintMessage] @customMessage = @ErrMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0
			
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
																								AND (cin.[project_id] = @projectID OR (@flgOptions & 268435456 = 268435456))
																								AND cin.[instance_name] LIKE @sqlServerNameFilter
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
										N'<TD WIDTH="120px" class="details" ALIGN="LEFT" nowrap>' + CASE WHEN @isClustered=0 THEN @machineName ELSE dbo.ufn_reportHTMLGetClusterNodeNames(@projectID, @instanceName, @flgOptions) END + N'</TD>' + 
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
	ELSE
		SET @HTMLReport = REPLACE(@HTMLReport, '{ErrorlogMessagesPermissionErrorsCount}', '(N/A)')


	-----------------------------------------------------------------------------------------------------
	--Errorlog Messages - Issues Detected
	-----------------------------------------------------------------------------------------------------
	IF (@flgActions & 16 = 16) AND (@flgOptions & 1048576 = 1048576)
		begin
			SET @ErrMessage = 'Build Report: Errorlog Messages - Issues Detected'
			EXEC [dbo].[usp_logPrintMessage] @customMessage = @ErrMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0
			
			SET @HTMLReportArea=N''
			SET @HTMLReportArea = @HTMLReportArea + 
							N'<A NAME="ErrorlogMessagesIssuesDetected" class="category-style">Errorlog Messages - Issues Detected (last ' + CAST(@reportOptionErrorlogMessageLastHours AS [nvarchar]) + N'h)</A><br>
							<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="0px" class="no-border">
							<TR VALIGN=TOP>
								<TD class="small-size" COLLSPAN="7">limit messages per instance to maximum ' + CAST(@reportOptionErrorlogMessageLimit AS [nvarchar](32)) + N' </TD>
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
					IF @messageCount > @reportOptionErrorlogMessageLimit SET @messageCount = @reportOptionErrorlogMessageLimit
					SET @issuesDetectedCount = @issuesDetectedCount + @messageCount

					SET @HTMLReportArea = @HTMLReportArea + 
								N'<TR VALIGN="TOP" class="' + CASE WHEN @idx & 1 = 1 THEN 'color-2' ELSE 'color-1' END + '">' + 
										N'<TD WIDTH="200px" class="details" ALIGN="LEFT" nowrap ROWSPAN="' + CAST(@messageCount AS [nvarchar](64)) + '"><A NAME="ErrorlogMessagesIssuesDetected' + @instanceName + N'">' + @instanceName + N'</A></TD>' 

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
	ELSE
		SET @HTMLReport = REPLACE(@HTMLReport, '{ErrorlogMessagesIssuesDetectedCount}', '(N/A)')

	
	-----------------------------------------------------------------------------------------------------
	--OS Event Messages - Permission Errors
	-----------------------------------------------------------------------------------------------------
	IF (@flgActions & 32 = 32) AND (@flgOptions & 67108864 = 67108864)
		begin
			SET @ErrMessage = 'Build Report: OS Event Messages - Permission Errors'
			EXEC [dbo].[usp_logPrintMessage] @customMessage = @ErrMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0
			
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
																								AND (cin.[project_id] = @projectID OR (@flgOptions & 268435456 = 268435456))
																								AND cin.[instance_name] LIKE @sqlServerNameFilter
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
	ELSE
		SET @HTMLReport = REPLACE(@HTMLReport, '{OSEventMessagesPermissionErrorsCount}', '(N/A)')


	-----------------------------------------------------------------------------------------------------
	--OS Event messages - Issues Detected
	-----------------------------------------------------------------------------------------------------
	IF (@flgActions & 32 = 32) AND (@flgOptions & 134217728 = 134217728)
		begin
			SET @ErrMessage = 'Build Report: OS Event messages - Issues Detected'
			EXEC [dbo].[usp_logPrintMessage] @customMessage = @ErrMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0
			
			SET @HTMLReportArea=N''
			SET @HTMLReportArea = @HTMLReportArea + 
							N'<A NAME="OSEventMessagesIssuesDetected" class="category-style">OS Events Messages - Issues Detected (last ' + CAST(@reportOptionOSEventMessageLastHours AS [nvarchar]) + N'h)</A><br>
							<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="0px" class="no-border">
							<TR VALIGN=TOP>
								<TD class="small-size" COLLSPAN="7">limit messages per machine to maximum ' + CAST(@reportOptionOSEventMessageLimit AS [nvarchar](32)) + N' </TD>
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

			SET @issuesDetectedCount = 0 
			
			DECLARE crsOSEventMessagesInstanceName CURSOR LOCAL FAST_FORWARD FOR	SELECT DISTINCT
																							  [machine_name]
																							, COUNT(*) AS [messages_count]
																					FROM #filteredStatsOSEventMessagesDetail
																					GROUP BY [machine_name]
																					ORDER BY [machine_name]
			OPEN crsOSEventMessagesInstanceName
			FETCH NEXT FROM crsOSEventMessagesInstanceName INTO @machineName, @messageCount
			WHILE @@FETCH_STATUS=0
				begin
					IF @messageCount > @reportOptionOSEventMessageLimit SET @messageCount = @reportOptionOSEventMessageLimit
					SET @issuesDetectedCount = @issuesDetectedCount + @messageCount

					SET @HTMLReportArea = @HTMLReportArea + 
								N'<TR VALIGN="TOP" class="' + CASE WHEN @idx & 1 = 1 THEN 'color-2' ELSE 'color-1' END + '">' + 
										N'<TD WIDTH="200px" class="details" ALIGN="LEFT" nowrap ROWSPAN="' + CAST(@messageCount AS [nvarchar](64)) + '"><A NAME="OSEventMessagesIssuesDetected' + @machineName + N'">' + @machineName + N'</A></TD>' 

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
													SELECT  TOP (@reportOptionOSEventMessageLimit)
															[time_created], [log_type_desc], [level_desc], 
															[event_id], [record_id], [source], [message]
															, ROW_NUMBER() OVER(ORDER BY [time_created], [record_id]) [row_no]
															, SUM(1) OVER() AS [row_count]
													FROM #filteredStatsOSEventMessagesDetail
													WHERE	[machine_name] = @machineName
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
	ELSE
		SET @HTMLReport = REPLACE(@HTMLReport, '{OSEventMessagesIssuesDetectedCount}', '(N/A)')


	-----------------------------------------------------------------------------------------------------
	--Databases Status - Complete Details
	-----------------------------------------------------------------------------------------------------
	IF (@flgActions & 2 = 2) AND (@flgOptions & 8 = 8)
		begin
			SET @ErrMessage = 'Build Report: Databases Status - Complete Details'
			EXEC [dbo].[usp_logPrintMessage] @customMessage = @ErrMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0
		
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
																							AND (cin.[project_id] = @projectID OR (@flgOptions & 268435456 = 268435456))
																							AND cin.[instance_name] LIKE @sqlServerNameFilter
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
													SELECT    DISTINCT
															  cdn.[database_name], cdn.[state_desc]
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
															AND (cin.[project_id] = @projectID OR (@flgOptions & 268435456 = 268435456))
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
	IF (@flgActions & 4 = 4) AND (@flgOptions & 64 = 64)
		begin
			SET @ErrMessage = 'Build Report: SQL Server Agent Jobs Status - Complete Details'
			EXEC [dbo].[usp_logPrintMessage] @customMessage = @ErrMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0
			
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
			
			DECLARE crsSQLServerAgentJobsInstanceName CURSOR LOCAL FAST_FORWARD FOR	SELECT	ssajh.[instance_name], COUNT(DISTINCT [job_name] + CONVERT(varchar(20), ISNULL([last_execution_utc], GETUTCDATE()), 120)) AS [job_count]
																					FROM	[health-check].[vw_statsSQLAgentJobsHistory] ssajh
																					INNER JOIN [dbo].[vw_catalogInstanceNames] cin ON cin.[project_id] = ssajh.[project_id] AND cin.[instance_id] = ssajh.[instance_id]
																					LEFT JOIN [report].[htmlSkipRules] rsr ON	rsr.[module] = 'health-check'
																																AND rsr.[rule_id] = 32
																																AND rsr.[active] = 1
																																AND (rsr.[skip_value]=ssajh.[instance_name])
																					WHERE	(cin.[project_id] = @projectID OR (@flgOptions & 268435456 = 268435456))
																							AND cin.[instance_name] LIKE @sqlServerNameFilter
																							AND cin.[instance_active]=1
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
													SELECT [job_name], [last_execution_status], [last_execution_date], [last_execution_time], [message]
															, ROW_NUMBER() OVER(ORDER BY [job_name]) [row_no]
															, SUM(1) OVER() AS [row_count]
													FROM (
															SELECT	DISTINCT
																	[job_name], [last_execution_status], [last_execution_date], [last_execution_time], [message]
															FROM	[health-check].[vw_statsSQLAgentJobsHistory]
															WHERE	([project_id] = @projectID OR (@flgOptions & 268435456 = 268435456))
																	AND [instance_name] = @instanceName
														)Y
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
			SET @ErrMessage = 'Build Report: Disk Space Information - Complete Details'
			EXEC [dbo].[usp_logPrintMessage] @customMessage = @ErrMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0

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
											<TH WIDTH="120px" class="details-bold" nowrap>Total Size (GB)</TH>
											<TH WIDTH="120px" class="details-bold" nowrap>Available Space (GB)</TH>
											<TH WIDTH="120px" class="details-bold" nowrap>Percent Available (%)</TH>'

			SET @idx=1		

			SET @volumeCount = 0
			DECLARE crsDiskSpaceInformationMachineNames CURSOR LOCAL FAST_FORWARD FOR	SELECT DISTINCT
																								  cin.[machine_name]
																								, cin.[is_clustered]
																								, cin.[cluster_node_machine_name]
																								, COUNT(DISTINCT [volume_mount_point]) AS [volume_count]
																						FROM [dbo].[vw_catalogInstanceNames]  cin
																						INNER JOIN 
																								(
																								 SELECT  DISTINCT 
																									  	  dsi.[instance_id]
																										, dsi.[project_id] 
																										, dsi.[logical_drive]
																										, dsi.[volume_mount_point]
																										, MAX(dsi.[total_size_mb])/1024		 AS [total_size_gb]
																										, MIN(dsi.[available_space_mb])/1024 AS [available_space_gb]
																										, MIN(dsi.[percent_available])	AS [percent_available]
																								 FROM [health-check].[vw_statsDiskSpaceInfo]		dsi
																								 GROUP BY  dsi.[instance_id], dsi.[project_id], dsi.[logical_drive], dsi.[volume_mount_point]
																								)dsi	ON dsi.[project_id] = cin.[project_id] AND dsi.[instance_id] = cin.[instance_id]
																						LEFT JOIN [report].[htmlSkipRules] rsr ON	rsr.[module] = 'health-check'
																																	AND rsr.[rule_id] = 65536
																																	AND rsr.[active] = 1
																																	AND (rsr.[skip_value] = cin.[machine_name] OR rsr.[skip_value]=cin.[instance_name])
																						WHERE cin.[instance_active]=1
																								AND (cin.[project_id] = @projectID OR (@flgOptions & 268435456 = 268435456))
																								AND cin.[instance_name] LIKE @sqlServerNameFilter
																								AND rsr.[id] IS NULL	
																						GROUP BY cin.[machine_name], cin.[is_clustered], cin.[cluster_node_machine_name]
																						ORDER BY cin.[machine_name]
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
													N'<TD WIDTH="120px" class="details" ALIGN="RIGHT" nowrap>' + ISNULL(CAST(CAST(ROUND([total_size_gb], 0) AS [int]) AS [nvarchar](64)), N'&nbsp;')+ N'</TD>' + 
													N'<TD WIDTH="120px" class="details" ALIGN="RIGHT" nowrap>' + ISNULL(CAST(CAST(ROUND([available_space_gb], 0) AS [int]) AS [nvarchar](64)), N'&nbsp;')+ N'</TD>' + 
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
																, MAX(dsi.[total_size_mb])/1024		 AS [total_size_gb]
																, MIN(dsi.[available_space_mb])/1024 AS [available_space_gb]
																, MIN(dsi.[percent_available])	AS [percent_available]
																, ROW_NUMBER() OVER(ORDER BY dsi.[logical_drive], dsi.[volume_mount_point]) [row_no]
																, SUM(1) OVER() AS [row_count]
													FROM [dbo].[vw_catalogInstanceNames] cin
													INNER JOIN 
															(
																SELECT  DISTINCT 
																		dsi.[instance_id]
																	, dsi.[project_id] 
																	, dsi.[logical_drive]
																	, dsi.[volume_mount_point]
																	, MAX(dsi.[total_size_mb])		AS [total_size_mb]
																	, MIN(dsi.[available_space_mb]) AS [available_space_mb]
																	, MIN(dsi.[percent_available])	AS [percent_available]
																FROM [health-check].[vw_statsDiskSpaceInfo]		dsi
																GROUP BY  dsi.[instance_id], dsi.[project_id], dsi.[logical_drive], dsi.[volume_mount_point]
															) dsi	ON dsi.[project_id] = cin.[project_id] AND dsi.[instance_id] = cin.[instance_id]
													WHERE	cin.[instance_active]=1
															AND (cin.[project_id] = @projectID OR (@flgOptions & 268435456 = 268435456))
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

	SET @reportID=SCOPE_IDENTITY()
		
	-----------------------------------------------------------------------------------------------------
	--save HTML report to external file
	-----------------------------------------------------------------------------------------------------
	IF (SELECT [host_platform] FROM [dbo].[vw_catalogInstanceNames]	WHERE [instance_name] = @@SERVERNAME) <> 'linux'
		begin
			IF @reportFileName IS NOT NULL AND LEFT(@reportFileName, 1) = '+'
				SET @HTMLReportFileName = REPLACE(REPLACE(@HTMLReportFileName, '.html', ''), '.htm', '') + '_' + CAST(@reportID AS [nvarchar]) + SUBSTRING(@reportFileName, 2, LEN(@reportFileName)-1) + '.html'
			ELSE
				SET @HTMLReportFileName = REPLACE(REPLACE(@HTMLReportFileName, '.html', ''), '.htm', '') + '_' + CAST(@reportID AS [nvarchar]) + '.html'

			
			SET @reportFilePath='"' + @localStoragePath + @HTMLReportFileName + '"'
	

			-----------------------------------------------------------------------------------------------------
			DECLARE @optionXPValue				[int]

			/* enable xp_cmdshell configuration option */
			EXEC [dbo].[usp_changeServerOption_xp_cmdshell]   @serverToRun	 = @@SERVERNAME
															, @flgAction	 = 1			-- 1=enable | 0=disable
															, @optionXPValue = @optionXPValue OUTPUT
															, @debugMode	 = 0

			/* save report using bcp */	
			SET @queryToRun=N'master.dbo.xp_cmdshell ''bcp "SELECT [html_content] FROM ' + [dbo].[ufn_getObjectQuoteName](DB_NAME(), 'quoted') + '.[report].[htmlContent] WHERE [id]=' + CAST(@reportID AS [varchar]) + '" queryout ' + @reportFilePath + ' -c ' + CASE WHEN SERVERPROPERTY('InstanceName') IS NOT NULL THEN N'-S ' + @@SERVERNAME ELSE N'' END + N' -T'''
			EXEC sp_executesql  @queryToRun

			/* disable xp_cmdshell configuration option */
			EXEC [dbo].[usp_changeServerOption_xp_cmdshell]   @serverToRun	 = @@SERVERNAME
															, @flgAction	 = 0			-- 1=enable | 0=disable
															, @optionXPValue = @optionXPValue OUTPUT
															, @debugMode	 = 0

			IF @@ERROR=0
				UPDATE [report].[htmlContent]
					SET   [html_content] = NULL
						, [file_name]	 = @HTMLReportFileName
				WHERE [id] = @reportID
		end
	ELSE
		UPDATE [report].[htmlContent]
			SET   [file_path] = ''
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
								'<file_name>' + [dbo].[ufn_getObjectQuoteName](ISNULL(@HTMLReportFileName,''), 'xml') + '</file_name>' + 
								CASE WHEN @HTTPAddress IS NOT NULL THEN '<http_address>' + [dbo].[ufn_getObjectQuoteName](@HTTPAddress, 'xml') + '</http_address>' ELSE '' END + 
								'<relative_path>' + [dbo].[ufn_getObjectQuoteName](ISNULL(@relativeStoragePath,''), 'xml') + '</relative_path>' + 
								'</detail></report-html>'

	IF (@sendReportAsAttachment=1) OR (@HTTPAddress IS NULL)
		begin
			SET @file_attachments	= REPLACE(@reportFilePath, '"', '')
			EXEC [dbo].[usp_logPrintMessage] @customMessage = @reportFilePath, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0
			
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
