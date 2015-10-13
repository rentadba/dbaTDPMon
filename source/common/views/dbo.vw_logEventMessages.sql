-----------------------------------------------------------------------------------------------------
--
-----------------------------------------------------------------------------------------------------
RAISERROR('Create view : [dbo].[vw_logEventMessages]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (SELECT * FROM sys.views WHERE object_id = OBJECT_ID(N'[dbo].[vw_logEventMessages]'))
DROP VIEW [dbo].[vw_logEventMessages]
GO

CREATE VIEW [dbo].[vw_logEventMessages]
/* WITH ENCRYPTION */
AS

-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 08.01.2015
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================

SELECT    am.[id]		AS [event_message_id]
		, am.[remote_event_id]
		, cin.[project_id] 
		, cin.[id]		AS [instance_id]
		, cin.[name]	AS [instance_name]
		, am.[event_date_utc]
		, am.[module]
		, am.[parameters]
		, am.[event_name]
		, am.[database_name]
		, am.[object_name]
		, am.[child_object_name]
		, am.[message]	
		, CAST(am.[message]	AS [xml]) AS [message_xml]
		, am.[send_email_to]
		, am.[event_type]
		, CASE am.[event_type]	WHEN 0 THEN 'info' 
								WHEN 1 THEN 'alert' 
								WHEN 2 THEN 'job-history'
								WHEN 3 THEN 'report-html'
								WHEN 4 THEN 'action' 
								WHEN 5 THEN 'backup-job-history'
								WHEN 6 THEN 'alert-custom' 
								ELSE NULL END AS [event_type_desc]
		, am.[is_email_sent]
		, am.[flood_control]
FROM [dbo].[logEventMessages]		am
LEFT JOIN [dbo].[catalogInstanceNames]		 cin	ON cin.[id] = am.[instance_id] AND cin.[project_id] = am.[project_id]
GO
