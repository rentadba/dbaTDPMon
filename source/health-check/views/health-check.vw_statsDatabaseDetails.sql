RAISERROR('Create view : [health-check].[vw_statsDatabaseDetails]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (SELECT * FROM sys.views WHERE object_id = OBJECT_ID(N'[health-check].[vw_statsDatabaseDetails]'))
DROP VIEW [health-check].[vw_statsDatabaseDetails]
GO

CREATE VIEW [health-check].[vw_statsDatabaseDetails]
/* WITH ENCRYPTION */
AS

-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 05.12.2014
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================

SELECT 	  cin.[project_id]		AS [project_id]
		, cin.[id]				AS [instance_id]
		, cin.[name]			AS [instance_name]
		, shcdd.[catalog_database_id]
		, cdn.[database_id]
		, cdn.[name]			AS [database_name]
		, cdn.[active]
		, cdn.[state]
		, cdn.[state_desc] 
		, shcdd.[data_size_mb] + shcdd.[log_size_mb] AS [size_mb]
		, shcdd.[data_size_mb]
		, shcdd.[data_space_used_percent]
		, shcdd.[log_size_mb]
		, shcdd.[log_space_used_percent]
		, shcdd.[is_auto_close]
		, shcdd.[is_auto_shrink]
		, shcdd.[physical_drives]
		, shcdd.[last_backup_time]
		, shcdd.[last_dbcc checkdb_time]
		, CASE shcdd.[recovery_model] WHEN 1 THEN 'FULL' WHEN 2 THEN 'BULK_LOGGED' WHEN 3 THEN 'SIMPLE' ELSE NULL END AS [recovery_model_desc]
		, CASE shcdd.[page_verify_option] WHEN 0 THEN 'NONE' WHEN 1 THEN 'TORN_PAGE_DETECTION' WHEN 2 THEN 'CHECKSUM' ELSE NULL END AS [page_verify_option_desc]
		, shcdd.[compatibility_level]
		, shcdd.[is_growth_limited]
		, shcdd.[event_date_utc]
FROM [dbo].[catalogInstanceNames]	cin	
INNER JOIN [dbo].[catalogDatabaseNames] cdn ON cin.[id] = cdn.[instance_id] AND cin.[project_id] = cdn.[project_id]
INNER JOIN [health-check].[statsDatabaseDetails] shcdd ON shcdd.[catalog_database_id] = cdn.[id] AND shcdd.[instance_id] = cdn.[instance_id]
GO
