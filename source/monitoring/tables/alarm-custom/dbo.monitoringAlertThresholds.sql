-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 13.10.2015
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================

-----------------------------------------------------------------------------------------------------
--
-----------------------------------------------------------------------------------------------------
RAISERROR('Drop table: [dbo].[monitoringAlertThresholds]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[monitoringAlertThresholds]') AND type in (N'U'))
DROP TABLE [dbo].[monitoringAlertThresholds]
GO
