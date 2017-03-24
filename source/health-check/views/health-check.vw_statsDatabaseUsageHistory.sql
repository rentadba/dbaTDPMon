RAISERROR('Create view : [health-check].[vw_statsDatabaseUsageHistory]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (SELECT * FROM sys.views WHERE object_id = OBJECT_ID(N'[health-check].[vw_statsDatabaseUsageHistory]'))
DROP VIEW [health-check].[vw_statsDatabaseUsageHistory]
GO

CREATE VIEW [health-check].[vw_statsDatabaseUsageHistory]
/* WITH ENCRYPTION */
AS

-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 21.03.2017
-- Module			 : Database Analysis & Performance health-check
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
		, shcdd.[physical_drives]
		, shcdd.[event_date_utc]
FROM [dbo].[catalogInstanceNames]	cin	
INNER JOIN [dbo].[catalogDatabaseNames] cdn ON cin.[id] = cdn.[instance_id] AND cin.[project_id] = cdn.[project_id]
INNER JOIN [health-check].[statsDatabaseUsageHistory] shcdd ON shcdd.[catalog_database_id] = cdn.[id] AND shcdd.[instance_id] = cdn.[instance_id]
GO
