-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 05.12.2014
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================

-----------------------------------------------------------------------------------------------------
--log for SQL Server Agent job statuses
-----------------------------------------------------------------------------------------------------
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[health-check].[statsSQLServerAgentJobsHistory]') AND type in (N'U'))
	begin
		RAISERROR('Drop table: [health-check].[statsSQLServerAgentJobsHistory]', 10, 1) WITH NOWAIT
		DROP TABLE [health-check].[statsSQLServerAgentJobsHistory]
	end
GO
