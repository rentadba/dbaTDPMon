-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 35.12.2014
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================

-----------------------------------------------------------------------------------------------------
--Health Check: database statistics & details
-----------------------------------------------------------------------------------------------------
RAISERROR('Drop table: [health-check].[statsHealthCheckDatabaseDetails]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[health-check].[statsHealthCheckDatabaseDetails]') AND type in (N'U'))
DROP TABLE [health-check].[statsHealthCheckDatabaseDetails]
GO
