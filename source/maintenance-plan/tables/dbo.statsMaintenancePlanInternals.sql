-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 06.02.2015
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================
-- table will contain actions made against schema objects, in order to track/troubleshoot
-----------------------------------------------------------------------------------------------------

-----------------------------------------------------------------------------------------------------
--
-----------------------------------------------------------------------------------------------------
RAISERROR('Create table: [maintenance-plan].[statsMaintenancePlanInternals]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[maintenance-plan].[statsMaintenancePlanInternals]') AND type in (N'U'))
DROP TABLE [maintenance-plan].[statsMaintenancePlanInternals]
GO
CREATE TABLE [maintenance-plan].[statsMaintenancePlanInternals]
(
	[id]				[bigint] IDENTITY (1, 1)NOT NULL,
	[event_date_utc]	[datetime]				NOT NULL CONSTRAINT [DF_statsMaintenancePlanInternals_EventDateUTC] DEFAULT (GETUTCDATE()),
	[session_id]		[smallint]				NOT NULL CONSTRAINT [DF_statsMaintenancePlanInternals_SessionID] DEFAULT (@@SPID),
	[name]				[sysname]				NOT NULL,
	[server_name]		[sysname]				NOT NULL,
	[database_name]		[sysname]				NULL,
	[schema_name]		[sysname]				NULL,
	[object_name]		[sysname]				NULL,
	[child_object_name]	[sysname]				NULL,
	CONSTRAINT [PK_statsMaintenancePlanInternals] PRIMARY KEY  CLUSTERED 
	(
		[id]
	) ON [FG_Statistics_Data]
) ON [FG_Statistics_Data]
GO

CREATE INDEX [IX_statsMaintenancePlanInternals_SessionID_Name] ON [maintenance-plan].[statsMaintenancePlanInternals]
		([session_id], [name]) 
	INCLUDE 
		([server_name], [database_name]) 
	ON [FG_Statistics_Index]
GO

CREATE INDEX [IX_statsMaintenancePlanInternals_Name] ON [maintenance-plan].[statsMaintenancePlanInternals]
		([name], [server_name], [database_name])
	ON [FG_Statistics_Index]
GO