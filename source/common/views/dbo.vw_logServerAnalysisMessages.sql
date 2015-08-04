-----------------------------------------------------------------------------------------------------
--
-----------------------------------------------------------------------------------------------------
RAISERROR('Create view : [dbo].[vw_logServerAnalysisMessages]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (SELECT * FROM sys.views WHERE object_id = OBJECT_ID(N'[dbo].[vw_logServerAnalysisMessages]'))
DROP VIEW [dbo].[vw_logServerAnalysisMessages]
GO

CREATE VIEW [dbo].[vw_logServerAnalysisMessages]
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
		, lsam.[event_date_utc]
		, lsam.[descriptor]
		, lsam.[message]
FROM [dbo].[logServerAnalysisMessages]	lsam
INNER JOIN [dbo].[catalogInstanceNames] cin ON cin.[id] = lsam.[instance_id] AND cin.[project_id] = lsam.[project_id]
GO

