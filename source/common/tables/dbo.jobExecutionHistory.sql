-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 21.03.2017
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================

-----------------------------------------------------------------------------------------------------
--internal job definition queue (used for internal job parallelism)
-----------------------------------------------------------------------------------------------------
RAISERROR('Create table: [dbo].[jobExecutionHistory]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[jobExecutionHistory]') AND type in (N'U'))
DROP TABLE [dbo].[jobExecutionHistory]
GO
CREATE TABLE [dbo].[jobExecutionHistory]
(
	[id]					[int]	 IDENTITY (1, 1)	NOT NULL,
	[instance_id]			[smallint]		NOT NULL,
	[project_id]			[smallint]		NOT NULL,
	[module]				[varchar](32)	NOT NULL,
	[descriptor]			[varchar](256)	NOT NULL,
	[filter]				[sysname]		NULL,
	[task_id]				[bigint]		NULL,
	[database_name]			[sysname]		NULL,
	[for_instance_id]		[smallint]		NOT NULL,
	[job_name]				[sysname]		NOT NULL,
	[job_id]				[uniqueidentifier] NULL,
	[job_step_name]			[sysname]		NOT NULL,
	[job_database_name]		[sysname]		NOT NULL,
	[job_command]			[nvarchar](max) NOT NULL,
	[execution_date]		[datetime]		NULL,
	[running_time_sec]		[bigint]		NULL,
	[log_message]			[nvarchar](max) NULL,
	[status]				[smallint]		NOT NULL CONSTRAINT [DF_jobExecutionHistory_Status] DEFAULT (-1),
	[event_date_utc]		[datetime]		NOT NULL CONSTRAINT [DF_jobExecutionHistory_EventDateUTC] DEFAULT (GETUTCDATE()),
	CONSTRAINT [PK_jobExecutionHistory] PRIMARY KEY  CLUSTERED 
	(
		[id]
	) ON [FG_Statistics_Data],
	CONSTRAINT [FK_jobExecutionHistory_catalogProjects] FOREIGN KEY 
	(
		[project_id]
	) 
	REFERENCES [dbo].[catalogProjects] 
	(
		[id]
	),
	CONSTRAINT [FK_jobExecutionHistory_ForInstanceID_catalogInstanceNames] FOREIGN KEY 
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

CREATE INDEX [IX_jobExecutionHistory_InstanceID] ON [dbo].[jobExecutionHistory]([instance_id], [project_id]) ON [FG_Statistics_Index]
GO
CREATE INDEX [IX_jobExecutionHistory_ProjectID] ON [dbo].[jobExecutionHistory] ([project_id], [event_date_utc]) INCLUDE ([instance_id]) ON [FG_Statistics_Index]
GO
CREATE INDEX [IX_jobExecutionHistory_JobName] ON [dbo].[jobExecutionHistory]([job_name], [job_step_name]) ON [FG_Statistics_Index]
GO
CREATE INDEX [IX_jobExecutionHistory_Descriptor] ON [dbo].[jobExecutionHistory]([project_id], [status], [module], [descriptor]) INCLUDE ([instance_id], [for_instance_id], [job_name]) ON [FG_Statistics_Index];
GO
CREATE INDEX [IX_jobExecutionHistory] ON [dbo].[jobExecutionHistory] ([module], [for_instance_id], [project_id], [instance_id], [job_name], [job_step_name], [filter]) ON [FG_Statistics_Index];
GO
