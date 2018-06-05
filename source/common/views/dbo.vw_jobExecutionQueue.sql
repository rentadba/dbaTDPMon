-----------------------------------------------------------------------------------------------------
--
-----------------------------------------------------------------------------------------------------
RAISERROR('Create view : [dbo].[vw_jobExecutionQueue]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (SELECT * FROM sys.views WHERE object_id = OBJECT_ID(N'[dbo].[vw_jobExecutionQueue]'))
DROP VIEW [dbo].[vw_jobExecutionQueue]
GO

CREATE VIEW [dbo].[vw_jobExecutionQueue]
/* WITH ENCRYPTION */
AS

-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 21.09.2015
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================

SELECT    jeq.[id]
		, jeq.[project_id]
		, cp.[code]		AS [project_code]
		, jeq.[instance_id]
		, (SELECT cin.[name] FROM [dbo].[catalogInstanceNames] cin WHERE cin.[id] = jeq.[instance_id]) AS [instance_name]
		, jeq.[for_instance_id]
		, cinF.[name]	AS [for_instance_name]
		, jeq.[module]
		, jeq.[descriptor]
		, jeq.[filter]
		, jeq.[task_id]
		, jeq.[database_name]
		, jeq.[job_name]
		, jeq.[job_step_name]
		, jeq.[job_database_name]
		, jeq.[job_command]
		, jeq.[execution_date]
		, jeq.[running_time_sec]
		, jeq.[status]
		, CASE jeq.[status] WHEN '-1' THEN 'Not executed'
							WHEN '0' THEN 'Failed'
							WHEN '1' THEN 'Succeded'				
							WHEN '2' THEN 'Retry'
							WHEN '3' THEN 'Canceled'
							WHEN '4' THEN 'In progress'
							ELSE 'Unknown'
			END AS [status_desc]
		, jeq.[log_message]
		, jeq.[event_date_utc]
		, jeq.[priority]
FROM [dbo].[jobExecutionQueue]		jeq
INNER JOIN [dbo].[catalogInstanceNames]	 cinF	ON cinF.[id] = jeq.[for_instance_id] AND cinF.[project_id] = jeq.[project_id]
INNER JOIN [dbo].[catalogProjects]		 cp		ON cp.[id] = jeq.[project_id]
GO



