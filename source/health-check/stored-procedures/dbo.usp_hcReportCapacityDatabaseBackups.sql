SET QUOTED_IDENTIFIER ON 
GO
SET ANSI_NULLS ON 
GO

RAISERROR('Create procedure: [dbo].[usp_hcReportCapacityDatabaseBackups]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (
	    SELECT * 
	      FROM sys.objects 
	     WHERE object_id = OBJECT_ID(N'[dbo].[usp_hcReportCapacityDatabaseBackups]') 
	       AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[usp_hcReportCapacityDatabaseBackups]
GO

CREATE PROCEDURE [dbo].[usp_hcReportCapacityDatabaseBackups]
		@projectCode			[varchar](32) = '%',
		@sqlServerNameFilter	[sysname] = '%',
		@daysToAnalyze			[smallint] = 7
AS

-- ============================================================================
-- Copyright (c) 2004-2019 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 08.06.2019
-- Module			 : Database Analysis & Performance health-check
-- Description		 : capacity report: database(s) and backup count/size statistics 
-- ============================================================================
SET NOCOUNT ON

DECLARE   @startEvent		[datetime]

SET @startEvent = DATEADD(day, -@daysToAnalyze, GETUTCDATE())
SET @startEvent = CONVERT([datetime], CONVERT([varchar](10), @startEvent, 120), 120)

IF OBJECT_ID('tempdb..#backupData') IS NOT NULL DROP TABLE #backupData

/* extract backupset information */
SELECT	  lem.[project_id], lem.[instance_id], lem.[instance_name], lem.[database_name]
		, info.value ('size_bytes[1]', 'bigint') as [size_bytes]
		, info.value ('file_name[1]', 'sysname') as [file_name]
		, info.value ('type[1]', 'nvarchar(16)') as [backup_type]
		, lem.[event_date_utc]
INTO #backupData
FROM (
		SELECT l.[project_id], l.[instance_id], l.[instance_name], l.[database_name], l.[message_xml], l.[event_date_utc]
		FROM [dbo].[vw_logEventMessages] l
		INNER JOIN [dbo].[vw_catalogProjects] p ON l.[project_id] = p.[project_id]
		WHERE l.[module]='dbo.usp_mpDatabaseBackup'
			AND l.[event_name]='database backup'
			AND l.[event_type]=0
			AND l.[message] LIKE '<backupset>%'
			AND l.[event_date_utc] >= @startEvent
			AND p.[project_code] LIKE @projectCode
			AND l.[instance_name] LIKE @sqlServerNameFilter
	) lem
CROSS APPLY [message_xml].nodes('//backupset/detail') M(info)

CREATE INDEX [IX_#backupData] ON #backupData([project_id], [instance_id], [database_name]) INCLUDE ([instance_name], [file_name], [backup_type], [size_bytes])

;WITH databaseSize AS
(
	/* compute database count and size */
	SELECT cdn.[instance_name], cp.[solution_name], cp.[is_production]
			, COUNT(DISTINCT shcdd.[id]) AS [database_count]
			, CAST(SUM(COALESCE((shcdd.[data_size_mb] + shcdd.[log_size_mb]), ag.[database_size_mb])) /1024. AS [numeric](38,3)) AS [database_size_gb]
	FROM [health-check].[statsDatabaseDetails] shcdd
	INNER JOIN [dbo].[vw_catalogDatabaseNames] cdn ON	shcdd.[instance_id] = cdn.[instance_id]
													AND shcdd.[catalog_database_id] = cdn.[catalog_database_id]
	INNER JOIN [dbo].[vw_catalogProjects] cp ON cdn.[project_id] = cp.[project_id]
	LEFT JOIN
		(
			/* non-readable replicas - take db size from primary replica */
			SELECT a.[project_id], a.[database_name], b.[secondary_instance_id], b.[secondary_catalog_database_id], a.[database_size_mb]
			FROM (
					/* AG primary database size */
					SELECT p.[project_id], p.[database_name], p.[instance_id], p.[catalog_database_id]
							, (shcdd.[data_size_mb] + shcdd.[log_size_mb]) AS [database_size_mb]
					FROM [health-check].[vw_statsDatabaseAlwaysOnDetails] p
					INNER JOIN [health-check].[vw_statsDatabaseDetails] shcdd ON shcdd.[instance_id] = p.[instance_id]
																			AND shcdd.[catalog_database_id] = p.[catalog_database_id]
					WHERE p.[role_desc] = 'PRIMARY'
				)a
			INNER JOIN
				(
					/* AlwaysOn Availability Groups databases pairs */
					SELECT	p.[project_id], p.[database_name],
							p.[instance_id] AS [primary_instance_id], p.[catalog_database_id] AS [primary_catalog_database_id],
							s.[instance_id] AS [secondary_instance_id], s.[catalog_database_id] AS [secondary_catalog_database_id]
					FROM [health-check].[vw_statsDatabaseAlwaysOnDetails] p
					INNER JOIN [health-check].[vw_statsDatabaseAlwaysOnDetails] s ON p.[project_id] = s.[project_id]
																					AND p.[cluster_name] = s.[cluster_name]
																					AND p.[ag_name] = s.[ag_name]
																					AND p.[database_name] = s.[database_name]
					WHERE p.[role_desc] = 'PRIMARY'
							AND s.[role_desc] = 'SECONDARY'
				)b ON a.[project_id] = b.[project_id]
						AND a.[catalog_database_id] = b.[primary_catalog_database_id]
						AND a.[instance_id] = b.[primary_instance_id]			
		) ag ON ag.[project_id] = cdn.[project_id]
				AND ag.[secondary_catalog_database_id] = cdn.[catalog_database_id]
				AND ag.[secondary_instance_id] = cdn.[instance_id]
	WHERE cdn.[active] = 1
			AND cdn.[project_code] LIKE @projectCode
			AND cdn.[instance_name] LIKE @sqlServerNameFilter
	GROUP BY cdn.[instance_name], cp.[solution_name], cp.[is_production]
),
backupSize AS
(
	/* compute backup size per solution/type */
	SELECT bd.[instance_name], cp.[solution_name], cp.[is_production], bd.[backup_type]
			, COUNT(DISTINCT [file_name]) AS [file_count]
			, CAST(SUM(bd.[size_bytes]) / (1024 * 1024 * 1024.) AS [numeric](38,3)) AS [backup_size_gb]
	FROM #backupData bd
	INNER JOIN [dbo].[catalogDatabaseNames] cdn ON	bd.[project_id] = cdn.[project_id]
													AND bd.[instance_id] = cdn.[instance_id]
													AND bd.[database_name] = cdn.[name]
	INNER JOIN [dbo].[vw_catalogProjects] cp ON cdn.[project_id] = cp.[project_id]
	GROUP BY bd.[instance_name], cp.[solution_name], cp.[is_production], bd.[backup_type]
),
backupSizePerType AS
(
	/* compute backup size per type*/
	SELECT [instance_name], [solution_name], [is_production]
			, SUM(ISNULL([full], 0)) AS [full_backup_gb]
			, SUM(ISNULL([diff], 0)) AS [diff_backup_gb]
			, SUM(ISNULL([log], 0)) AS [log_backup_gb]
	FROM backupSize
	PIVOT
		(	SUM([backup_size_gb]) 
			FOR [backup_type] in ([full], [diff], [log])
		)X
	GROUP BY [instance_name], [solution_name], [is_production]
),
backupFileCount AS
(
	SELECT [instance_name], [solution_name], [is_production]
			, SUM([file_count]) AS [file_count]
	FROM backupSize
	GROUP BY [instance_name], [solution_name], [is_production]
)
SELECT	d.[instance_name], d.[solution_name], d.[is_production], 
		d.[database_count], d.[database_size_gb], 
		ISNULL(b.[full_backup_gb] + b.[diff_backup_gb] + b.[log_backup_gb], 0) AS [backup_size_gb],
		ISNULL(c.[file_count], 0) AS [backup_files_count],
		ISNULL(b.[full_backup_gb], 0) AS [full_backup_gb],
		ISNULL(b.[diff_backup_gb], 0) AS [diff_backup_gb],
		ISNULL(b.[log_backup_gb], 0) AS [log_backup_gb]
FROM databaseSize d
LEFT JOIN backupSizePerType b ON d.[instance_name] = b.[instance_name] AND ISNULL(d.[solution_name], '') = ISNULL(b.[solution_name], '') AND d.[is_production] = b.[is_production]
LEFT JOIN backupFileCount c ON d.[instance_name] = c.[instance_name] AND ISNULL(d.[solution_name], '') = ISNULL(c.[solution_name], '') AND d.[is_production] = c.[is_production]
ORDER BY d.[instance_name], d.[solution_name], d.[is_production]
GO

/*
EXEC [dbo].[usp_hcReportCapacityDatabaseBackups]	@projectCode		= '%',
													@sqlServerNameFilter= '%',
													@daysToAnalyze		= 7
*/
