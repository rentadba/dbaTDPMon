RAISERROR('Create view : [health-check].[vw_statsDiskSpaceInfo]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (SELECT * FROM sys.views WHERE object_id = OBJECT_ID(N'[health-check].[vw_statsDiskSpaceInfo]'))
DROP VIEW [health-check].[vw_statsDiskSpaceInfo]
GO

CREATE VIEW [health-check].[vw_statsDiskSpaceInfo]
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
		, dsi.[event_date_utc]
		, dsi.[logical_drive]
		, dsi.[volume_mount_point]
		, dsi.[total_size_mb]
		, dsi.[available_space_mb]
		, dsi.[percent_available]
		, dsi.[block_size]
FROM [health-check].[statsDiskSpaceInfo]	dsi
INNER JOIN [dbo].[catalogInstanceNames] cin ON cin.[id] = dsi.[instance_id] AND cin.[project_id] = dsi.[project_id]
GO
