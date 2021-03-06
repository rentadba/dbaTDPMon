-----------------------------------------------------------------------------------------------------
--
-----------------------------------------------------------------------------------------------------
RAISERROR('Create view : [dbo].[vw_jobExecutionStatistics]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (SELECT * FROM sys.views WHERE object_id = OBJECT_ID(N'[dbo].[vw_jobExecutionStatistics]'))
DROP VIEW [dbo].[vw_jobExecutionStatistics]
GO

CREATE VIEW [dbo].[vw_jobExecutionStatistics]
/* WITH ENCRYPTION */
AS
-- ============================================================================
-- Copyright (c) 2004-2021 Rent a DBA (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Razvan Puscasu / Dan Andrei STEFAN
-- Create date		 : 
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================
SELECT    jeq.[project_id]
		, cp.[code] AS [project_code]
		, jeq.[for_instance_id] AS [instance_id]
		, cin.[name] AS [instance_name]
		, jeq.[module]
		, jeq.[descriptor]
		, DATEDIFF(minute, MIN(jeq.[execution_date]), MAX(DATEADD(SECOND, jeq.[running_time_sec], jeq.[execution_date]))) AS [duration_minutes_parallel]
		, SUM(jeq.[running_time_sec]) / 60 AS [duration_minutes_serial]
		, CASE WHEN MIN(jeq.[execution_date]) IS NOT NULL THEN MIN(jeq.[execution_date]) ELSE DATEADD(mi, DATEDIFF(mi, GETUTCDATE(), GETDATE()), MIN(jeq.[event_date_utc])) END AS [start_date]
		, ISNULL(it.[id], -1) AS [task_id]
		, it.[task_name]
		, CASE	WHEN COUNT(*) = SUM(jeq.[status]) 
				THEN 'success'
				ELSE CASE WHEN MAX(jeq.[status]) IN (4) OR SUM(CASE WHEN jeq.[status] = -1 THEN 1 ELSE 0 END) <> COUNT(*)				
						  THEN 'partial: ' 
						  ELSE 'not executed: '
					 END + CASE WHEN SUM(CASE WHEN jeq.[status] = 1 THEN 1 ELSE 0 END) > 0
								THEN 'success (' + CAST(SUM(CASE WHEN jeq.[status] = 1 THEN 1 ELSE 0 END) AS [varchar](10)) + ' of ' + CAST(COUNT(*) AS [varchar](10)) + '); '
								ELSE ''
						   END
						 + CASE WHEN SUM(CASE WHEN jeq.[status] = 0 THEN 1 ELSE 0 END) > 0
								THEN 'failed (' + CAST(SUM(CASE WHEN jeq.[status] = 0 THEN 1 ELSE 0 END) AS [varchar](10)) + ' of ' + CAST(COUNT(*) AS [varchar](10)) + ')' 
								ELSE ''
						   END
		 END AS [status]
FROM [dbo].[jobExecutionQueue] AS jeq
INNER JOIN [dbo].[catalogProjects] AS cp ON cp.[id] = jeq.[project_id]
INNER JOIN [dbo].[catalogInstanceNames] AS cin ON cin.[id] = jeq.[for_instance_id] AND cin.[project_id]=jeq.[project_id]
LEFT  JOIN [dbo].[vw_catalogDatabaseNames] AS cdn ON cdn.[project_id] = jeq.[project_id] 
													AND cdn.[instance_id] = jeq.[for_instance_id]
													AND cdn.[database_name] = jeq.[database_name]
INNER JOIN [dbo].[appInternalTasks] AS it ON it.[id] = jeq.[task_id]
WHERE	(	
			(
				 cdn.[database_id] IS NOT NULL 
			 AND cdn.[active]=1 
			 AND [module]='maintenance-plan'
			 AND CHARINDEX('(dbid=' + CAST(cdn.[database_id] AS sysname) + ')', jeq.[job_name])>1
			) 
		 OR (
			 [module]<>'maintenance-plan'
			)
		)
		AND jeq.[running_time_sec] IS NOT NULL
GROUP BY jeq.[project_id], cp.[code], jeq.[module], jeq.[descriptor], jeq.[for_instance_id], cin.[name], it.[id], it.[task_name]

UNION ALL

SELECT    jeq.[project_id]
		, cp.[code] AS [project_code]
		, jeq.[for_instance_id] AS [instance_id]
		, cin.[name] AS [instance_name]
		, jeq.[module]
		, jeq.[descriptor]
		, NULL AS [duration_minutes_parallel]
		, NULL AS [duration_minutes_serial]
		, NULL AS [start_date]
		, ISNULL(it.[id], -1) AS [task_id]
		, it.[task_name]
		, CASE	WHEN COUNT(*) = SUM(jeq.[status]) 
				THEN 'success'
				ELSE CASE WHEN MAX(jeq.[status]) IN (4) OR SUM(CASE WHEN jeq.[status] = -1 THEN 1 ELSE 0 END) <> COUNT(*)				
						  THEN 'partial: ' 
						  ELSE 'not executed: '
					 END + CASE WHEN SUM(CASE WHEN jeq.[status] = 1 THEN 1 ELSE 0 END) > 0
								THEN 'success (' + CAST(SUM(CASE WHEN jeq.[status] = 1 THEN 1 ELSE 0 END) AS [varchar](10)) + ' of ' + CAST(COUNT(*) AS [varchar](10)) + '); '
								ELSE ''
						   END
						 + CASE WHEN SUM(CASE WHEN jeq.[status] = 0 THEN 1 ELSE 0 END) > 0
								THEN 'failed (' + CAST(SUM(CASE WHEN jeq.[status] = 0 THEN 1 ELSE 0 END) AS [varchar](10)) + ' of ' + CAST(COUNT(*) AS [varchar](10)) + ')' 
								ELSE ''
						   END
		 END AS [status]
FROM [dbo].[jobExecutionQueue] AS jeq
INNER JOIN [dbo].[catalogProjects] AS cp ON cp.[id] = jeq.[project_id]
INNER JOIN [dbo].[catalogInstanceNames] AS cin ON cin.[id] = jeq.[for_instance_id] AND cin.[project_id]=jeq.[project_id]
LEFT  JOIN [dbo].[vw_catalogDatabaseNames] AS cdn ON cdn.[project_id] = jeq.[project_id] 
													AND cdn.[instance_id] = jeq.[for_instance_id]
													AND cdn.[database_name] = jeq.[database_name]
INNER JOIN [dbo].[appInternalTasks] AS it ON it.[id] = jeq.[task_id]
WHERE	(	
			(
				 cdn.[database_id] IS NOT NULL 
			 AND cdn.[active]=1 
			 AND [module]='maintenance-plan'
			 AND CHARINDEX('(dbid=' + CAST(cdn.[database_id] AS sysname) + ')', jeq.[job_name])>1
			) 
		 OR (
			 [module]<>'maintenance-plan'
			)
		)
		AND jeq.[running_time_sec] IS NULL
GROUP BY jeq.[project_id], cp.[code], jeq.[module], jeq.[descriptor], jeq.[for_instance_id], cin.[name], it.[id], it.[task_name]
GO
