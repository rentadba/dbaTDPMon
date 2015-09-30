-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 08.01.2015
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================

-----------------------------------------------------------------------------------------------------
RAISERROR('Create table: [dbo].[logEventMessages]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (SELECT * FROM sysobjects WHERE id = OBJECT_ID(N'[dbo].[logEventMessages]') AND type in (N'U'))
DROP TABLE [dbo].[logEventMessages]
GO

DECLARE @SQLMajorVersion [int]
SELECT @SQLMajorVersion = REPLACE(LEFT(ISNULL(CAST(SERVERPROPERTY('ProductVersion') AS [varchar](32)), ''), 2), '.', '') 


DECLARE @queryToRun [varchar](8000)

SET @queryToRun = '
CREATE TABLE [dbo].[logEventMessages]
(
	[id]										[bigint] IDENTITY (1, 1)NOT NULL,
	[remote_event_id]							[bigint]			NULL,
	[project_id]								[smallint]			NULL,
	[instance_id]								[smallint]			NULL,
	[event_date_utc]							[datetime]			NOT NULL,
	[module]									[sysname]			NOT NULL,
	[parameters]								[nvarchar](512)			NULL,
	[event_name]								[nvarchar](256)		NOT NULL,
	[database_name]								[sysname]				NULL,
	[object_name]								[nvarchar](261)			NULL,
	[child_object_name]							[sysname]				NULL,
	[message]									[varchar](' + CASE WHEN @SQLMajorVersion>8 THEN 'max' ELSE '4000' END + ')			NULL,
	[send_email_to]								[varchar](1024)			NULL,
	[event_type]								[smallint]				NULL,
	[is_email_sent]								[bit]				NOT NULL CONSTRAINT [DF_logEventMessages_is_email_sent] DEFAULT (0),
	[flood_control]								[bit]				NOT NULL CONSTRAINT [DF_logEventMessages_flood_control] DEFAULT (0),
	CONSTRAINT [PK_logEventMessages] PRIMARY KEY  CLUSTERED 
	(
		[id]
	) ON [FG_Statistics_Data],
	CONSTRAINT [FK_logEventMessages_catalogProjects] FOREIGN KEY 
	(
		[project_id]
	) 
	REFERENCES [dbo].[catalogProjects] 
	(
		[id]
	),
	CONSTRAINT [FK_logEventMessages_catalogInstanceNames] FOREIGN KEY 
	(
		[instance_id],
		[project_id]
	) 
	REFERENCES [dbo].[catalogInstanceNames] 
	(
		[id],
		[project_id]
	)

) ON [FG_Statistics_Data]'

EXEC (@queryToRun)

SET @queryToRun = 'CREATE INDEX [IX_logEventMessages_InstanceID] ON [dbo].[logEventMessages]([instance_id], [project_id])' + CASE WHEN @SQLMajorVersion>8 THEN ' INCLUDE ([remote_event_id])' ELSE '' END + ' ON [FG_Statistics_Index]'
EXEC (@queryToRun)

SET @queryToRun = 'CREATE INDEX [IX_logEventMessages_EventName_EventDate] ON [dbo].[logEventMessages]([event_name], [event_date_utc]) ON [FG_Statistics_Index]'
EXEC (@queryToRun)

SET @queryToRun = 'CREATE INDEX [IX_logEventMessages_ObjectName] ON [dbo].[logEventMessages]([object_name], [database_name]) ON [FG_Statistics_Index]'
EXEC (@queryToRun)

SET @queryToRun = 'CREATE INDEX [IX_logEventMessages_EventType] ON [dbo].[logEventMessages]([event_type]) ON [FG_Statistics_Index]'
EXEC (@queryToRun)

SET @queryToRun = 'CREATE INDEX [IX_logEventMessages_Module_EventName] ON [dbo].[logEventMessages] ([project_id], [instance_id], [module], [event_name])' + CASE WHEN @SQLMajorVersion>8 THEN ' INCLUDE ([parameters], [database_name], [object_name], [child_object_name], [event_date_utc])' ELSE '' END + ' ON [FG_Statistics_Index]'
EXEC (@queryToRun)
GO
