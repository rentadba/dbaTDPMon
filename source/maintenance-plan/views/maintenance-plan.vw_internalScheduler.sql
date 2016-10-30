RAISERROR('Create view : [maintenance-plan].[vw_internalScheduler]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (SELECT * FROM sysobjects WHERE id = OBJECT_ID(N'[maintenance-plan].[vw_internalScheduler]') AND type in (N'V'))
DROP VIEW [maintenance-plan].[vw_internalScheduler]
GO

CREATE VIEW [maintenance-plan].[vw_internalScheduler]
/* WITH ENCRYPTION */
AS

-- ============================================================================
-- Copyright (c) 2004-2016 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 25.10.2016
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================

SELECT 	  cp.[id]				AS [project_id]
		, cp.[code]				AS [project_code]
		, cp.[name]				AS [project_name]
		, isch.[id]
		, isch.[task_id]
		, it.[job_descriptor]
		, it.[task_name]
		, isch.[scheduled_weekday]
		, isch.[active]
FROM [maintenance-plan].[internalScheduler] isch
INNER JOIN [maintenance-plan].[internalTasks] it ON it.[id] = isch.[task_id]
LEFT  JOIN [dbo].[catalogProjects] cp ON cp.[id] = isch.[project_id]
GO
