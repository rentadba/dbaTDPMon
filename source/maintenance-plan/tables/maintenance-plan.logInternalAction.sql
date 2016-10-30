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
RAISERROR('Create table: [maintenance-plan].[logInternalAction]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (SELECT * FROM sysobjects WHERE id = OBJECT_ID(N'[maintenance-plan].[logInternalAction]') AND type in (N'U'))
DROP TABLE [maintenance-plan].[logInternalAction]
GO

CREATE TABLE [maintenance-plan].[logInternalAction]
(
	[id]				[bigint] IDENTITY (1, 1)NOT NULL,
	[event_date_utc]	[datetime]				NOT NULL CONSTRAINT [DF_logInternalAction_EventDateUTC] DEFAULT (GETUTCDATE()),
	[session_id]		[smallint]				NOT NULL CONSTRAINT [DF_logInternalAction_SessionID] DEFAULT (@@SPID),
	[name]				[sysname]				NOT NULL,
	[server_name]		[sysname]				NOT NULL,
	[database_name]		[sysname]				NULL,
	[schema_name]		[sysname]				NULL,
	[object_name]		[sysname]				NULL,
	[child_object_name]	[sysname]				NULL,
	CONSTRAINT [PK_logInternalAction] PRIMARY KEY  CLUSTERED 
	(
		[id]
	) ON [FG_Statistics_Data]
) ON [FG_Statistics_Data]
GO

CREATE INDEX [IX_logInternalAction_SessionID_Name] ON [maintenance-plan].[logInternalAction]
		([session_id], [name]) 
	INCLUDE 
		([server_name], [database_name]) 
	ON [FG_Statistics_Index]
GO

CREATE INDEX [IX_logInternalAction_Name] ON [maintenance-plan].[logInternalAction]
		([name], [server_name], [database_name])
	ON [FG_Statistics_Index]
GO