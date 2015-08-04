-----------------------------------------------------------------------------------------------------
--
-----------------------------------------------------------------------------------------------------
RAISERROR('Create view : [dbo].[vw_catalogInstanceNames]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (SELECT * FROM sysobjects WHERE id = OBJECT_ID(N'[dbo].[vw_catalogInstanceNames]') AND type in (N'V'))
DROP VIEW [dbo].[vw_catalogInstanceNames]
GO

CREATE VIEW [dbo].[vw_catalogInstanceNames]
/* WITH ENCRYPTION */
AS

-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 05.12.2014
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================

SELECT 	  cp.[id]				AS [project_id]
		, cp.[code]				AS [project_code]
		, cp.[name]				AS [project_name]
		, cmn.[id]				AS [machine_id]
		, cmn.[name] + CASE WHEN cmn.[domain] IS NOT NULL THEN '.' + cmn.[domain] ELSE '' END AS [machine_name]
		, cin.[id]				AS [instance_id]
		, cin.[name]			AS [instance_name]
		, cin.[version]
		, cin.[edition]
		, cin.[is_clustered]	AS [is_clustered]
		, cin.[active]			AS [instance_active]
		, cmnA.[id]				AS [cluster_node_machine_id]
		, cmnA.[name]			AS [cluster_node_machine_name]
		, cin.[last_refresh_date_utc]
FROM [dbo].[catalogProjects]			cp
INNER JOIN [dbo].[catalogMachineNames]	cmn ON cp.[id] = cmn.[project_id]
INNER JOIN [dbo].[catalogInstanceNames]	cin	ON cmn.[id] = cin.[machine_id] AND cmn.[project_id] = cin.[project_id]
LEFT  JOIN [dbo].[catalogMachineNames]	cmnA ON cmnA.[id] = cin.[cluster_node_machine_id] AND cmnA.[project_id] = cin.[project_id]
GO
