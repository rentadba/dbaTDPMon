-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 04.09.2015
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================

-----------------------------------------------------------------------------------------------------
--OS Events
-----------------------------------------------------------------------------------------------------
RAISERROR('Create table: [health-check].[statsOSEventLogs]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[health-check].[statsOSEventLogs]') AND type in (N'U'))
DROP TABLE [health-check].[statsOSEventLogs]
GO
CREATE TABLE [health-check].[statsOSEventLogs]
(
	[id]					[int]	 IDENTITY (1, 1)	NOT NULL,
	[instance_id]			[smallint]		NOT NULL,
	[project_id]			[smallint]		NOT NULL,
	[machine_id]			[smallint]		NOT NULL,
	[event_date_utc]		[datetime]		NOT NULL,
	[log_type_id]			[tinyint]		NOT NULL,
	[event_id]				[int]				NULL,
	[level_id] 				[tinyint]			NULL,
	[record_id]				[bigint]			NULL,
	[category_id]			[int]				NULL,
	[category_name]			[nvarchar](256)		NULL,
	[source] 				[nvarchar](512)		NULL,
	[process_id]			[int]				NULL,
	[thread_id]				[int]				NULL,
	[machine_name]			[sysname]			NULL,
	[user_id]				[nvarchar](256)		NULL,
	[time_created]			[varchar](32)		NULL,
	[time_created_utc]		[datetime]			NULL,
	[message] 				[nvarchar](max)		NULL
	CONSTRAINT [PK_statsOSEventLogs] PRIMARY KEY  CLUSTERED 
	(
		[id],
		[instance_id]
	) ON [FG_Statistics_Data],
	CONSTRAINT [FK_statsOSEventLogs_catalogProjects] FOREIGN KEY 
	(
		[project_id]
	) 
	REFERENCES [dbo].[catalogProjects] 
	(
		[id]
	),
	CONSTRAINT [FK_statsOSEventLogs_catalogInstanceNames] FOREIGN KEY 
	(
		[instance_id],
		[project_id]
	) 
	REFERENCES [dbo].[catalogInstanceNames] 
	(
		[id],
		[project_id]
	),
	CONSTRAINT [FK_statsOSEventLogs_catalogMachineNames] FOREIGN KEY 
	(
		[machine_id],
		[project_id]
	) 
	REFERENCES [dbo].[catalogMachineNames] 
	(
		[id],
		[project_id]
	)

)ON [FG_Statistics_Data]
GO

CREATE INDEX [IX_statsOSEventLogs_InstanceID] ON [health-check].[statsOSEventLogs]([instance_id], [project_id]) ON [FG_Statistics_Index]
GO
CREATE INDEX [IX_statsOSEventLogs_ProjecteID] ON [health-check].[statsOSEventLogs]([project_id]) ON [FG_Statistics_Index]
GO
CREATE INDEX [IX_statsOSEventLogs_MachineID] ON [health-check].[statsOSEventLogs]([machine_id], [project_id]) ON [FG_Statistics_Index]
GO
