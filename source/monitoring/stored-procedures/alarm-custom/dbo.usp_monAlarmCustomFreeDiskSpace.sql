RAISERROR('Create procedure: [dbo].[usp_monAlarmCustomFreeDiskSpace]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (
	    SELECT * 
	      FROM sys.objects 
	     WHERE object_id = OBJECT_ID(N'[dbo].[usp_monAlarmCustomFreeDiskSpace]') 
	       AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[usp_monAlarmCustomFreeDiskSpace]
GO

CREATE PROCEDURE [dbo].[usp_monAlarmCustomFreeDiskSpace]
		@projectCode		[varchar](32)=NULL,
		@sqlServerName		[sysname]='%'
/* WITH ENCRYPTION */
AS

-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 13.10.2015
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================
-- Change Date	: 
-- Description	: 
-----------------------------------------------------------------------------------------

SET NOCOUNT ON 

DECLARE	  @projectID						[int]
		, @warningFreeDiskMinPercent		[numeric](6,2)
		, @warningFreeDiskMinSpaceMB		[numeric](18,3)
		, @criticalFreeDiskMinPercent		[numeric](6,2)
		, @criticalFreeDiskMinSpaceMB		[numeric](18,3)
		, @ErrMessage						[nvarchar](256)
		
-----------------------------------------------------------------------------------------------------
--get default project code
IF @projectCode IS NULL
	SELECT	@projectCode = [value]
	FROM	[dbo].[appConfigurations]
	WHERE	[name] = 'Default project code'
			AND [module] = 'common'

SELECT    @projectID = [id]
FROM [dbo].[catalogProjects]
WHERE [code] = @projectCode 

IF @projectID IS NULL
	begin
		SET @ErrMessage=N'The value specifief for Project Code is not valid.'
		RAISERROR(@ErrMessage, 16, 1) WITH NOWAIT
	end

-----------------------------------------------------------------------------------------------------
SELECT	@warningFreeDiskMinPercent = [warning_limit]
FROM	[dbo].[monitoringAlertThresholds]
WHERE	[alert_name] = 'Logical Disk: Free Disk Space (%)'
		AND [category] = 'disk-space'
		AND [is_warning_limit_enabled]=1

SELECT	@criticalFreeDiskMinPercent = [critical_limit]
FROM	[dbo].[monitoringAlertThresholds]
WHERE	[alert_name] = 'Logical Disk: Free Disk Space (%)'
		AND [category] = 'disk-space'
		AND [is_critical_limit_enabled]=1

SELECT	@warningFreeDiskMinSpaceMB = [warning_limit]
FROM	[dbo].[monitoringAlertThresholds]
WHERE	[alert_name] = 'Logical Disk: Free Disk Space (MB)'
		AND [category] = 'disk-space'
		AND [is_warning_limit_enabled]=1

SELECT	@criticalFreeDiskMinSpaceMB = [critical_limit]
FROM	[dbo].[monitoringAlertThresholds]
WHERE	[alert_name] = 'Logical Disk: Free Disk Space (MB)'
		AND [category] = 'disk-space'
		AND [is_critical_limit_enabled]=1

-----------------------------------------------------------------------------------------------------

DECLARE   @instanceName		[sysname]
		, @objectName		[nvarchar](512)
		, @eventName		[sysname]
		, @severity			[sysname]
		, @eventMessage		[nvarchar](max)


DECLARE crsDiskSpaceAlarms CURSOR FOR	SELECT  DISTINCT
												  cin.[instance_name]
												, ISNULL(dsi.[volume_mount_point], dsi.[logical_drive]) AS [object_name]
												, 'warning'			AS [severity]
												, 'low disk space'	AS [event_name]
												, '<alert><detail>' + 
													'<severity>warning</severity>' + 
													'<machine_name>' + cin.[machine_name] + '</machine_name>' + 
													'<counter_name>low disk space</counter_name><target_name>' + ISNULL(dsi.[volume_mount_point], dsi.[logical_drive]) + '</target_name>' + 
													'<measure_unit>MB</measure_unit>' + 
													'<current_value>' + CAST(dsi.[available_space_mb] AS [varchar]) +'</current_value>' + 
													'<current_percentage>' + CAST(dsi.[percent_available] AS [varchar]) + '</current_percentage>' + 
													'<refference_value>' + CAST(dsi.[total_size_mb] AS [varchar]) + '</refference_value>' + 
													'<threshold_value>' + CAST(@warningFreeDiskMinSpaceMB AS [varchar]) + '</threshold_value>' + 
													'<threshold_percentage>' + CAST(@warningFreeDiskMinPercent AS [varchar]) + '</threshold_percentage>' + 
													'<event_date_utc>' + CONVERT([varchar](20), dsi.[event_date_utc], 120) + '</event_date_utc>' + 
													'</detail></alert>' AS [event_message]
										FROM [dbo].[vw_catalogInstanceNames]  cin
										INNER JOIN [dbo].[vw_statsDiskSpaceInfo]		dsi	ON dsi.[project_id] = cin.[project_id] AND dsi.[instance_id] = cin.[instance_id]
										LEFT  JOIN 
													(
														SELECT DISTINCT [project_id], [instance_id], [physical_drives] 
														FROM [dbo].[vw_statsHealthCheckDatabaseDetails]
													)   cdd ON cdd.[project_id] = cin.[project_id] AND cdd.[instance_id] = cin.[instance_id]
										LEFT JOIN [dbo].[reportHTMLSkipRules] rsr ON	rsr.[module] = 'monitoring'
																						AND rsr.[rule_id] = 1
																						AND rsr.[active] = 1
																						AND (rsr.[skip_value] = cin.[machine_name] OR rsr.[skip_value]=cin.[instance_name])
																						AND (   rsr.[skip_value2] IS NULL 
																							 OR (rsr.[skip_value2] IS NOT NULL AND rsr.[skip_value2] = ISNULL(dsi.[volume_mount_point], dsi.[logical_drive]))
																							)
										WHERE cin.[instance_active]=1
												AND cin.[project_id] = @projectID
												AND cin.[instance_name] LIKE @sqlServerName
												AND (    (	  dsi.[percent_available] IS NOT NULL 
															AND dsi.[percent_available] <= @warningFreeDiskMinPercent
															AND dsi.[percent_available] > @criticalFreeDiskMinPercent
															)
														OR 
														(	   dsi.[percent_available] IS NULL 
															AND dsi.[available_space_mb] IS NOT NULL 
															AND dsi.[available_space_mb] <= @warningFreeDiskMinSpaceMB
															AND dsi.[percent_available] > @criticalFreeDiskMinSpaceMB
														)
													)
												AND (dsi.[logical_drive] IN ('C') OR CHARINDEX(dsi.[logical_drive], cdd.[physical_drives])>0)
												AND rsr.[id] IS NULL
												AND (@warningFreeDiskMinSpaceMB IS NOT NULL AND @warningFreeDiskMinPercent IS NOT NULL)

										UNION ALL

										SELECT  DISTINCT
												  cin.[instance_name]
												, ISNULL(dsi.[volume_mount_point], dsi.[logical_drive]) AS [object_name]
												, 'critical'			AS [severity]
												, 'low disk space'	AS [event_name]
												, '<alert><detail>' + 
													'<severity>critical</severity>' + 
													'<machine_name>' + cin.[machine_name] + '</machine_name>' + 
													'<counter_name>low disk space</counter_name><target_name>' + ISNULL(dsi.[volume_mount_point], dsi.[logical_drive]) + '</target_name>' + 
													'<measure_unit>MB</measure_unit>' + 
													'<current_value>' + CAST(dsi.[available_space_mb] AS [varchar]) +'</current_value>' + 
													'<current_percentage>' + CAST(dsi.[percent_available] AS [varchar]) + '</current_percentage>' + 
													'<refference_value>' + CAST(dsi.[total_size_mb] AS [varchar]) + '</refference_value>' + 
													'<threshold_value>' + CAST(@criticalFreeDiskMinSpaceMB AS [varchar]) + '</threshold_value>' + 
													'<threshold_percentage>' + CAST(@criticalFreeDiskMinPercent AS [varchar]) + '</threshold_percentage>' + 
													'<event_date_utc>' + CONVERT([varchar](20), dsi.[event_date_utc], 120) + '</event_date_utc>' + 
													'</detail></alert>' AS [event_message]
										FROM [dbo].[vw_catalogInstanceNames]  cin
										INNER JOIN [dbo].[vw_statsDiskSpaceInfo]		dsi	ON dsi.[project_id] = cin.[project_id] AND dsi.[instance_id] = cin.[instance_id]
										LEFT  JOIN 
													(
														SELECT DISTINCT [project_id], [instance_id], [physical_drives] 
														FROM [dbo].[vw_statsHealthCheckDatabaseDetails]
													)   cdd ON cdd.[project_id] = cin.[project_id] AND cdd.[instance_id] = cin.[instance_id]
										LEFT JOIN [dbo].[reportHTMLSkipRules] rsr ON	rsr.[module] = 'monitoring'
																						AND rsr.[rule_id] = 1
																						AND rsr.[active] = 1
																						AND (rsr.[skip_value] = cin.[machine_name] OR rsr.[skip_value]=cin.[instance_name])
																						AND (   rsr.[skip_value2] IS NULL 
																							 OR (rsr.[skip_value2] IS NOT NULL AND rsr.[skip_value2] = ISNULL(dsi.[volume_mount_point], dsi.[logical_drive]))
																							)
										WHERE cin.[instance_active]=1
												AND cin.[project_id] = @projectID
												AND cin.[instance_name] LIKE @sqlServerName
												AND (    (	  dsi.[percent_available] IS NOT NULL 
															AND dsi.[percent_available] < @criticalFreeDiskMinPercent
															)
														OR 
														(	   dsi.[percent_available] IS NULL 
															AND dsi.[available_space_mb] IS NOT NULL 
															AND dsi.[available_space_mb] < @criticalFreeDiskMinSpaceMB
														)
													)
												AND (dsi.[logical_drive] IN ('C') OR CHARINDEX(dsi.[logical_drive], cdd.[physical_drives])>0)
												AND rsr.[id] IS NULL
												AND (@criticalFreeDiskMinSpaceMB IS NOT NULL AND @criticalFreeDiskMinPercent IS NOT NULL)
										
										ORDER BY [instance_name], [object_name]
OPEN crsDiskSpaceAlarms
FETCH NEXT FROM crsDiskSpaceAlarms INTO @instanceName, @objectName, @severity, @eventName, @eventMessage
WHILE @@FETCH_STATUS=0
	begin
		EXEC [dbo].[usp_logEventMessageAndSendEmail]	@projectCode			= @projectCode,
														@sqlServerName			= @instanceName,
														@dbName					= @severity,
														@objectName				= @objectName,
														@childObjectName		= NULL,
														@module					= 'health-check',
														@eventName				= @eventName,
														@parameters				= NULL,	
														@eventMessage			= @eventMessage,
														@dbMailProfileName		= NULL,
														@recipientsList			= NULL,
														@eventType				= 6,	/* 6 - alert-custom */
														@additionalOption		= 0

		FETCH NEXT FROM crsDiskSpaceAlarms INTO @instanceName, @objectName, @severity, @eventName, @eventMessage
	end
CLOSE crsDiskSpaceAlarms
DEALLOCATE crsDiskSpaceAlarms
GO
