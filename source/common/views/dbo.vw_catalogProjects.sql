-----------------------------------------------------------------------------------------------------
--
-----------------------------------------------------------------------------------------------------
RAISERROR('Create view : [dbo].[vw_catalogProjects]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (SELECT * FROM sysobjects WHERE id = OBJECT_ID(N'[dbo].[vw_catalogProjects]') AND type in (N'V'))
DROP VIEW [dbo].[vw_catalogProjects]
GO

CREATE VIEW [dbo].[vw_catalogProjects]
/* WITH ENCRYPTION */
AS

-- ============================================================================
-- Copyright (c) 2004-2018 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 05.04.2018
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================

SELECT 	  cp.[id]				AS [project_id]
		, cp.[code]				AS [project_code]
		, cp.[name]				AS [project_name]
		, cp.[description]		AS [project_description]
		, cp.[isProduction]		AS [is_production]
		, cp.[dbFilter]			AS [db_filter]
		, cs.[name]				AS [solution_name]
		, cs.[contact]
		, cs.[details]
		, cp.[active]
FROM [dbo].[catalogProjects]		cp
LEFT JOIN [dbo].[catalogSolutions]	cs ON cp.[solution_id] = cs.[id]
GO
