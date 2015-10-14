RAISERROR('Create view : [health-check].[vw_statsSQLServerErrorlogDetails]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (SELECT * FROM sys.views WHERE object_id = OBJECT_ID(N'[health-check].[vw_statsSQLServerErrorlogDetails]'))
DROP VIEW [health-check].[vw_statsSQLServerErrorlogDetails]
GO

CREATE VIEW [health-check].[vw_statsSQLServerErrorlogDetails]
/* WITH ENCRYPTION */
AS

-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 05.12.2014
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================

SELECT 	  sseld.[id]
		, cin.[project_id]		AS [project_id]
		, cin.[id]				AS [instance_id]
		, cin.[name]			AS [instance_name]
		, sseld.[log_date]
		, sseld.[process_info]
		, sseld.[text]
		, sseld.[event_date_utc]
FROM [dbo].[catalogInstanceNames]	cin	
INNER JOIN [health-check].[statsSQLServerErrorlogDetails] sseld ON cin.[id] = sseld.[instance_id] AND cin.[project_id] = sseld.[project_id]
GO
