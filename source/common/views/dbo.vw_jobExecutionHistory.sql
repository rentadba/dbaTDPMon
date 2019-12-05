-----------------------------------------------------------------------------------------------------
--
-----------------------------------------------------------------------------------------------------
RAISERROR('Create view : [dbo].[vw_jobExecutionHistory]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (SELECT * FROM sys.views WHERE object_id = OBJECT_ID(N'[dbo].[vw_jobExecutionHistory]'))
DROP VIEW [dbo].[vw_jobExecutionHistory]
GO

CREATE VIEW [dbo].[vw_jobExecutionHistory]
/* WITH ENCRYPTION */
AS

-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 21.09.2015
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================

/* previous / history executions */
SELECT    jeq.[id]
		, jeq.[project_id]
		, cp.[code]		AS [project_code]
		, jeq.[instance_id]
		, cin.[name]	AS [instance_name]
		, jeq.[for_instance_id]
		, cinF.[name]	AS [for_instance_name]
		, jeq.[module]
		, jeq.[descriptor]
		, jeq.[filter]
		, jeq.[task_id]
		, jeq.[database_name]
		, jeq.[job_name]
		, jeq.[job_id]
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
		, jeq.[remote_id]
FROM [dbo].[jobExecutionHistory]		 jeq
INNER JOIN [dbo].[catalogInstanceNames]	 cin	ON cin.[id] = jeq.[instance_id] --AND cin.[project_id] = jeq.[project_id]
LEFT  JOIN [dbo].[catalogInstanceNames]	 cinF	ON cinF.[id] = jeq.[for_instance_id] AND cinF.[project_id] = jeq.[project_id]
INNER JOIN [dbo].[catalogProjects]		 cp		ON cp.[id] = jeq.[project_id]

UNION ALL

/* current / last executions */
SELECT    jeq.[id]
		, jeq.[project_id]
		, cp.[code]		AS [project_code]
		, jeq.[instance_id]
		, cin.[name]	AS [instance_name]
		, jeq.[for_instance_id]
		, cinF.[name]	AS [for_instance_name]
		, jeq.[module]
		, jeq.[descriptor]
		, jeq.[filter]
		, jeq.[task_id]
		, jeq.[database_name]
		, jeq.[job_name]
		, jeq.[job_id]
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
		, NULL AS [remote_id]
FROM [dbo].[jobExecutionQueue]		jeq
INNER JOIN [dbo].[catalogInstanceNames]	 cin	ON cin.[id] = jeq.[instance_id] --AND cin.[project_id] = jeq.[project_id]
INNER JOIN [dbo].[catalogInstanceNames]	 cinF	ON cinF.[id] = jeq.[for_instance_id] AND cinF.[project_id] = jeq.[project_id]
INNER JOIN [dbo].[catalogProjects]		 cp		ON cp.[id] = jeq.[project_id]
GO
