RAISERROR('Create view : [health-check].[vw_statsDatabaseAlwaysOnDetails]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (SELECT * FROM sys.views WHERE object_id = OBJECT_ID(N'[health-check].[vw_statsDatabaseAlwaysOnDetails]'))
DROP VIEW [health-check].[vw_statsDatabaseAlwaysOnDetails]
GO

CREATE VIEW [health-check].[vw_statsDatabaseAlwaysOnDetails]
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
		, cmn.[id]				AS [machine_id]
		, cmn.[name]			AS [machine_name]
		, cin.[id]				AS [instance_id]
		, cin.[name]			AS [instance_name]
		, sdaod.[catalog_database_id]
		, cdn.[database_id]
		, cdn.[name]			AS [database_name]
		, cdn.[active]
		, cdn.[state]
		, cdn.[state_desc] 
		, sdaod.[cluster_name]
		, sdaod.[ag_name]
		, sdaod.[role_desc]
		, sdaod.[synchronization_health_desc]
		, sdaod.[synchronization_state_desc]
		, sdaod.[data_loss_sec]
		, sdaod.[event_date_utc]
FROM [dbo].[catalogInstanceNames]	cin	
INNER JOIN [dbo].[catalogMachineNames] cmn ON cmn.[id] = cin.[machine_id] AND cmn.[project_id] = cin.[project_id]
INNER JOIN [dbo].[catalogDatabaseNames] cdn ON cin.[id] = cdn.[instance_id] AND cin.[project_id] = cdn.[project_id]
INNER JOIN [health-check].[statsDatabaseAlwaysOnDetails] sdaod ON sdaod.[catalog_database_id] = cdn.[id] AND sdaod.[instance_id] = cdn.[instance_id]
GO



