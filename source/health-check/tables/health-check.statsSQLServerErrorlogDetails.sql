-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 29.05.2015
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================

-----------------------------------------------------------------------------------------------------
--log for SQL Server Agent job statuses
-----------------------------------------------------------------------------------------------------
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[health-check].[statsSQLServerErrorlogDetails]') AND type in (N'U'))
	begin
		RAISERROR('Drop table: [health-check].[statsSQLServerErrorlogDetails]', 10, 1) WITH NOWAIT
		DROP TABLE [health-check].[statsSQLServerErrorlogDetails]
	end
GO
