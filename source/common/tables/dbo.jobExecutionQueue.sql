-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 21.09.2015
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================

-----------------------------------------------------------------------------------------------------
--internal job definition queue (used for internal job parallelism)
-----------------------------------------------------------------------------------------------------
RAISERROR('Create table: [dbo].[jobExecutionQueue]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[jobExecutionQueue]') AND type in (N'U'))
DROP TABLE [dbo].[jobExecutionQueue]
GO
CREATE TABLE [dbo].[jobExecutionQueue]
(
	[id]					[int]	 IDENTITY (1, 1)	NOT NULL,
	[instance_id]			[smallint]		NOT NULL,
	[project_id]			[smallint]		NOT NULL,
	[module]				[varchar](32)	NOT NULL,
	[descriptor]			[varchar](256)	NOT NULL,
	[filter]				[sysname]		NULL,
	[for_instance_id]		[smallint]		NOT NULL,
	[job_name]				[sysname]		NOT NULL,
	[job_step_name]			[sysname]		NOT NULL,
	[job_database_name]		[sysname]		NOT NULL,
	[job_command]			[nvarchar](max) NOT NULL,
	[execution_date]		[datetime]		NULL,
	[running_time_sec]		[bigint]		NULL,
	[status]				[smallint]		NOT NULL CONSTRAINT [DF_jobExecutionQueue_Status] DEFAULT (-1),
	[event_date_utc]		[datetime]		NOT NULL CONSTRAINT [DF_jobExecutionQueue_EventDateUTC] DEFAULT (GETUTCDATE()),
	CONSTRAINT [PK_jobExecutionQueue] PRIMARY KEY  CLUSTERED 
	(
		[id]
	) ON [FG_Statistics_Data],
	CONSTRAINT [UK_jobExecutionQueue] UNIQUE
	(
		[for_instance_id],
		[project_id],
		[instance_id],
		[job_name],
		[job_step_name],
		[filter]
	) ON [FG_Statistics_Data],
	CONSTRAINT [FK_jobExecutionQueue_catalogProjects] FOREIGN KEY 
	(
		[project_id]
	) 
	REFERENCES [dbo].[catalogProjects] 
	(
		[id]
	),
	CONSTRAINT [FK_jobExecutionQueue_InstanceID_catalogInstanceNames] FOREIGN KEY 
	(
		[instance_id],
		[project_id]
	) 
	REFERENCES [dbo].[catalogInstanceNames] 
	(
		[id],
		[project_id]
	),
	CONSTRAINT [FK_jobExecutionQueue_ForInstanceID_catalogInstanceNames] FOREIGN KEY 
	(
		[for_instance_id],
		[project_id]
	) 
	REFERENCES [dbo].[catalogInstanceNames] 
	(
		[id],
		[project_id]
	)
)ON [FG_Statistics_Data]
GO

CREATE INDEX [IX_jobExecutionQueue_InstanceID] ON [dbo].[jobExecutionQueue]([instance_id], [project_id]) ON [FG_Statistics_Index]
GO
CREATE INDEX [IX_jobExecutionQueue_ForInstanceID] ON [dbo].[jobExecutionQueue]([for_instance_id], [project_id]) ON [FG_Statistics_Index]
GO
CREATE INDEX [IX_jobExecutionQueue_ProjectID] ON [dbo].[jobExecutionQueue] ([project_id], [event_date_utc]) INCLUDE ([instance_id]) ON [FG_Statistics_Index]
GO
CREATE INDEX [IX_jobExecutionQueue_JobName] ON [dbo].[jobExecutionQueue]([job_name], [job_step_name]) ON [FG_Statistics_Index]
GO
CREATE INDEX [IX_jobExecutionQueue_Descriptor] ON [dbo].[jobExecutionQueue]([project_id], [status], [module], [descriptor]) INCLUDE ([instance_id], [for_instance_id], [job_name]);
GO
