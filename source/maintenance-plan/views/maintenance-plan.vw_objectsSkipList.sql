RAISERROR('Create view : [maintenance-plan].[vw_objectSkipList]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (SELECT * FROM sysobjects WHERE id = OBJECT_ID(N'[maintenance-plan].[vw_objectSkipList]') AND type in (N'V'))
DROP VIEW [maintenance-plan].[vw_objectSkipList]
GO

CREATE VIEW [maintenance-plan].[vw_objectSkipList]
/* WITH ENCRYPTION */
AS

-- ============================================================================
-- Copyright (c) 2004-2016 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 14.06.2017
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================

SELECT 	  cp.[id]				AS [project_id]
		, cp.[code]				AS [project_code]
		, cp.[name]				AS [project_name]
		, osl.[id]
		, it.[descriptor]		AS [job_descriptor]
		, osl.[task_id]
		, it.[task_name]
		, osl.[schema_name]
		, osl.[object_name]
		, it.[flg_actions]
FROM [maintenance-plan].[objectSkipList] osl
INNER JOIN [dbo].[appInternalTasks] it ON it.[id] = osl.[task_id]
LEFT  JOIN [dbo].[catalogProjects] cp ON cp.[id] = osl.[project_id]
GO


