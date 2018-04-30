-----------------------------------------------------------------------------------------------------
--
-----------------------------------------------------------------------------------------------------
RAISERROR('Create view : [dbo].[vw_catalogDatabaseNames]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (SELECT * FROM sysobjects WHERE id = OBJECT_ID(N'[dbo].[vw_catalogDatabaseNames]') AND type in (N'V'))
DROP VIEW [dbo].[vw_catalogDatabaseNames]
GO

CREATE VIEW [dbo].[vw_catalogDatabaseNames]
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
		, cp.[code]				AS [project_code]
		, cin.[id]				AS [instance_id]
		, cin.[name]			AS [instance_name]
		, cdn.[id]				AS [catalog_database_id]
		, cdn.[database_id]
		, cdn.[name]			AS [database_name]
		, cdn.[active]
		, cdn.[state]
		, cdn.[state_desc] 
FROM [dbo].[catalogInstanceNames]	cin	
INNER JOIN [dbo].[catalogDatabaseNames] cdn ON cin.[id] = cdn.[instance_id] AND cin.[project_id] = cdn.[project_id]
INNER JOIN [dbo].[catalogProjects] cp ON cp.[id] = cin.[project_id]
GO

