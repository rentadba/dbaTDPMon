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

DECLARE @startDate [datetime]

SET @startDate = DATEADD(day, -@daysToAnalyze, GETUTCDATE())

SELECT	new.[instance_name], new.[database_name], 
		new.[size_mb] AS [current_size_mb], old.[size_mb] AS [old_size_mb],
		new.[data_size_mb] AS [current_data_size_mb], old.[data_size_mb] AS [old_data_size_mb],
		new.[log_size_mb] AS [current_log_size_mb], old.[log_size_mb] AS [old_log_size_mb],
		new.[size_mb] - old.[size_mb] AS [growth_size_mb],
		CAST((new.[data_size_mb] - old.[data_size_mb]) / old.[data_size_mb] * 100. AS [numeric](10,2)) AS [data_growth_percent]
FROM (
		SELECT [project_code], [database_name], [instance_name], [size_mb], [data_size_mb], [log_size_mb]
		FROM (
				SELECT	cdn.[project_code], 
						cdn.[database_name],
						cdn.[instance_name] AS [instance_name],
						(duh.[data_size_mb] + duh.[log_size_mb]) [size_mb], 
						(duh.[data_size_mb]) [data_size_mb], 
						(duh.[log_size_mb]) [log_size_mb],
						ROW_NUMBER() OVER(PARTITION BY cdn.[project_code], cdn.[database_name] ORDER BY duh.[event_date_utc]) AS [row_no]
				FROM [health-check].[statsDatabaseUsageHistory]	duh
				INNER JOIN [dbo].[vw_catalogDatabaseNames]		cdn ON cdn.[catalog_database_id] = duh.[catalog_database_id] AND cdn.[instance_id] = duh.[instance_id]
				LEFT JOIN [health-check].[vw_statsDatabaseAlwaysOnDetails] sddod ON sddod.[catalog_database_id] = duh.[catalog_database_id] AND sddod.[instance_id] = duh.[instance_id]
				WHERE	duh.[event_date_utc] >= @startDate
						AND cdn.[project_code] LIKE @projectCode
						AND cdn.[instance_name] LIKE @sqlServerNameFilter
						AND cdn.[active] = 1
						AND duh.[data_size_mb] IS NOT NULL
			)x
		WHERE [row_no] = 1	
	)old
INNER JOIN
	(
		SELECT [project_code], [database_name], [instance_name], [size_mb], [data_size_mb], [log_size_mb]
		FROM (
				SELECT	cdn.[project_code], 
						cdn.[database_name],
						cdn.[instance_name] AS [instance_name],
						(duh.[data_size_mb] + duh.[log_size_mb]) [size_mb], 
						(duh.[data_size_mb]) [data_size_mb], 
						(duh.[log_size_mb]) [log_size_mb],
						ROW_NUMBER() OVER(PARTITION BY cdn.[project_code], cdn.[database_name] ORDER BY duh.[event_date_utc] DESC) AS [row_no]
				FROM [health-check].[statsDatabaseUsageHistory]	duh
				INNER JOIN [dbo].[vw_catalogDatabaseNames]		cdn ON cdn.[catalog_database_id] = duh.[catalog_database_id] AND cdn.[instance_id] = duh.[instance_id]
				LEFT JOIN [health-check].[vw_statsDatabaseAlwaysOnDetails] sddod ON sddod.[catalog_database_id] = duh.[catalog_database_id] AND sddod.[instance_id] = duh.[instance_id]
				WHERE	duh.[event_date_utc] >= @startDate
						AND cdn.[project_code] LIKE @projectCode
						AND cdn.[instance_name] LIKE @sqlServerNameFilter
						AND cdn.[active] = 1
						AND duh.[data_size_mb] IS NOT NULL
			)y
		WHERE [row_no] = 1	
	)new ON new.[project_code] = old.[project_code] AND new.[database_name] = old.[database_name]
ORDER BY [growth_size_mb] DESC
GO

/*
EXEC [dbo].[usp_hcReportCapacityDatabaseGrowth]	@projectCode		 = '%',
												@sqlServerNameFilter = '%',
												@daysToAnalyze		 = 30
*/