-----------------------------------------------------------------------------------------------------
--
-----------------------------------------------------------------------------------------------------
RAISERROR('Create view : [dbo].[vw_jobExecutionStatisticsHistory]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (SELECT * FROM sys.views WHERE object_id = OBJECT_ID(N'[dbo].[vw_jobExecutionStatisticsHistory]'))
DROP VIEW [dbo].[vw_jobExecutionStatisticsHistory]
GO

CREATE VIEW [dbo].[vw_jobExecutionStatisticsHistory]
/* WITH ENCRYPTION */
AS

-- ============================================================================
-- Copyright (c) 2004-2018 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 11.04.2018
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================

/* previous / history executions */
SELECT	  jesh.[project_id]
		, cp.[code] AS [project_code]
		, jesh.[module]
		, jesh.[descriptor]
		, jesh.[duration_minutes_parallel]
		, jesh.[duration_minutes_serial]
		, jesh.[start_date]
		, jesh.[task_id]
		, jesh.[status]
FROM  [dbo].[jobExecutionStatisticsHistory] jesh
INNER JOIN [dbo].[catalogProjects] AS cp ON cp.[id] = jesh.[project_id]

UNION ALL

/* current / last executions */

SELECT    jes.[project_id]
		, jes.[project_code]
		, jes.[module]
		, jes.[descriptor]
		, jes.[duration_minutes_parallel]
		, jes.[duration_minutes_serial]
		, jes.[start_date]
		, jes.[task_id]
		, jes. [status]
FROM [dbo].[vw_jobExecutionStatistics] AS jes
GO

