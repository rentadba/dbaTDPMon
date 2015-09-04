RAISERROR('Create view : [dbo].[vw_statsOSEventLogs]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (SELECT * FROM sys.views WHERE object_id = OBJECT_ID(N'[dbo].[vw_statsOSEventLogs]'))
DROP VIEW [dbo].[vw_statsOSEventLogs]
GO

CREATE VIEW [dbo].[vw_statsOSEventLogs]
/* WITH ENCRYPTION */
AS

-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 04.09.2015
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================

SELECT 	  cin.[project_id]		AS [project_id]
		, cin.[id]				AS [instance_id]
		, cin.[name]			AS [instance_name]
		, cmn.[id]				AS [machine_id]
		, soel.[event_date_utc]
		, soel.[log_type_id]
		, CASE soel.[log_type_id] WHEN 1 THEN 'Application'
								  WHEN 2 THEN 'System'
								  WHEN 3 THEN 'Setup'
		  END AS [log_type_desc]
		, soel.[event_id]
		, soel.[level_id]
		, CASE soel.[level_id]	WHEN 1 THEN 'Critical'
								WHEN 2 THEN 'Error'
								WHEN 3 THEN 'Warning'
								WHEN 4 THEN 'Information'
		  END AS [level_desc]
		, soel.[record_id]
		, soel.[category_id]
		, soel.[category_name]
		, soel.[source]
		, soel.[process_id]
		, soel.[thread_id]
		, soel.[machine_name]
		, soel.[user_id]
		, soel.[time_created]
		, soel.[message]
FROM [dbo].[statsOSEventLogs]	soel
INNER JOIN [dbo].[catalogInstanceNames] cin ON cin.[id] = soel.[instance_id] AND cin.[project_id] = soel.[project_id]
INNER JOIN [dbo].[catalogMachineNames]  cmn ON cmn.[id] = soel.[machine_id] AND cmn.[project_id] = soel.[project_id]
GO
