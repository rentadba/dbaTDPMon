SET QUOTED_IDENTIFIER ON 
GO
SET ANSI_NULLS ON 
GO

RAISERROR('Create procedure: [dbo].[usp_hcReportCapacityDatabaseGrowth]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (
	    SELECT * 
	      FROM sys.objects 
	     WHERE object_id = OBJECT_ID(N'[dbo].[usp_hcReportCapacityDatabaseGrowth]') 
	       AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[usp_hcReportCapacityDatabaseGrowth]
GO

CREATE PROCEDURE [dbo].[usp_hcReportCapacityDatabaseGrowth]
		@projectCode			[varchar](32) = '%',
		@sqlServerNameFilter	[sysname] = '%',
		@daysToAnalyze			[smallint] = 30
AS

-- ============================================================================
-- Copyright (c) 2004-2019 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 08.06.2019
-- Module			 : Database Analysis & Performance health-check
-- Description		 : capacity report: database(s) growth over time 
-- ============================================================================
SET NOCOUNT ON

DECLARE @startWeek			[tinyint],
		@endWeek			[tinyint]

SET @startWeek = DATENAME(week, DATEADD(day, -@daysToAnalyze, GETUTCDATE()))
SET @endWeek   = DATENAME(week, GETUTCDATE())

SELECT	new.[instance_name], new.[database_name], 
		new.[size_mb] AS [current_size_mb], old.[size_mb] AS [old_size_mb],
		new.[data_size_mb] AS [current_data_size_mb], old.[data_size_mb] AS [old_data_size_mb],
		new.[log_size_mb] AS [current_log_size_mb], old.[log_size_mb] AS [old_log_size_mb],
		new.[size_mb] - old.[size_mb] AS [growth_size_mb],
		CAST((new.[data_size_mb] - old.[data_size_mb]) / old.[data_size_mb] * 100. AS [numeric](10,2)) AS [data_growth_percent]
FROM (
		SELECT	cdn.[project_code], 
				cdn.[database_name],
				MIN(cdn.[instance_name]) AS [instance_name],
				MIN(duh.[data_size_mb] + duh.[log_size_mb]) [size_mb], 
				MIN(duh.[data_size_mb]) [data_size_mb], 
				MIN(duh.[log_size_mb]) [log_size_mb]
		FROM [health-check].[statsDatabaseUsageHistory]	duh
		INNER JOIN [dbo].[vw_catalogDatabaseNames]		cdn ON cdn.[catalog_database_id] = duh.[catalog_database_id] AND cdn.[instance_id] = duh.[instance_id]
		LEFT JOIN [health-check].[vw_statsDatabaseAlwaysOnDetails] sddod ON sddod.[catalog_database_id] = duh.[catalog_database_id] AND sddod.[instance_id] = duh.[instance_id]
		WHERE	DATENAME(week, duh.[event_date_utc]) = @startWeek
				AND cdn.[project_code] LIKE @projectCode
				AND cdn.[instance_name] LIKE @sqlServerNameFilter
				AND cdn.[active] = 1
		GROUP BY cdn.[project_code], cdn.[database_name]
	)old
INNER JOIN
	(
		SELECT	cdn.[project_code], 
				cdn.[database_name],
				MIN(cdn.[instance_name]) AS [instance_name],
				MAX(duh.[data_size_mb] + duh.[log_size_mb]) [size_mb], 
				MAX(duh.[data_size_mb]) [data_size_mb], 
				MAX(duh.[log_size_mb]) [log_size_mb]
		FROM [health-check].[statsDatabaseUsageHistory]	duh 
		INNER JOIN [dbo].[vw_catalogDatabaseNames]		cdn	ON cdn.[catalog_database_id] = duh.[catalog_database_id] AND cdn.[instance_id] = duh.[instance_id]
		LEFT JOIN [health-check].[vw_statsDatabaseAlwaysOnDetails] sddod ON sddod.[catalog_database_id] = duh.[catalog_database_id] AND sddod.[instance_id] = duh.[instance_id]
		WHERE	DATENAME(week, duh.[event_date_utc]) = @endWeek
				AND cdn.[project_code] LIKE @projectCode
				AND cdn.[instance_name] LIKE @sqlServerNameFilter
				AND cdn.[active] = 1
		GROUP BY cdn.[project_code], cdn.[database_name]
	)new ON new.[project_code] = old.[project_code] AND new.[database_name] = old.[database_name]
WHERE new.[size_mb] > old.[size_mb]
ORDER BY [growth_size_mb] DESC
GO

/*
EXEC [dbo].[usp_hcReportCapacityDatabaseGrowth]	@projectCode		 = '%',
												@sqlServerNameFilter = '%',
												@daysToAnalyze		 = 30
*/