RAISERROR('Create view : [dbo].[vw_statsSQLServerAgentJobsHistory]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (SELECT * FROM sys.views WHERE object_id = OBJECT_ID(N'[dbo].[vw_statsSQLServerAgentJobsHistory]'))
DROP VIEW [dbo].[vw_statsSQLServerAgentJobsHistory]
GO

CREATE VIEW [dbo].[vw_statsSQLServerAgentJobsHistory]
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
		, ssajh.[event_date_utc]
		, ssajh.[job_name]
		, ssajh.[last_execution_status]
		, ssajh.[last_execution_date]
		, ssajh.[last_execution_time]
		, ssajh.[running_time_sec]
		, ssajh.[message]
FROM [dbo].[statsSQLServerAgentJobsHistory]	ssajh
INNER JOIN [dbo].[catalogInstanceNames] cin ON cin.[id] = ssajh.[instance_id] AND cin.[project_id] = ssajh.[project_id]
GO
