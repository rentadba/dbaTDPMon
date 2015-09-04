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
RAISERROR('Create table: [dbo].[statsSQLServerAgentJobsHistory]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[statsSQLServerAgentJobsHistory]') AND type in (N'U'))
DROP TABLE [dbo].[statsSQLServerAgentJobsHistory]
GO
CREATE TABLE [dbo].[statsSQLServerAgentJobsHistory]
(
	[id]					[int]	 IDENTITY (1, 1)	NOT NULL,
	[instance_id]			[smallint]		NOT NULL,
	[project_id]			[smallint]		NOT NULL,
	[event_date_utc]		[datetime]		NOT NULL,
	[job_name]				[sysname]		NOT NULL,
	[last_execution_status] [int]			NOT NULL,
	[last_execution_date]	[varchar](10)	NULL, 
	[last_execution_time]	[varchar](8)	NULL,
	[running_time_sec]		[bigint]		NULL,
	[message]				[varchar](max)	NULL, 
	CONSTRAINT [PK_statsSQLServerAgentJobsHistory] PRIMARY KEY  CLUSTERED 
	(
		[id],
		[instance_id]
	) ON [FG_Statistics_Data],
	CONSTRAINT [FK_statsSQLServerAgentJobsHistory_catalogProjects] FOREIGN KEY 
	(
		[project_id]
	) 
	REFERENCES [dbo].[catalogProjects] 
	(
		[id]
	),
	CONSTRAINT [FK_statsSQLServerAgentJobsHistory_catalogInstanceNames] FOREIGN KEY 
	(
		[instance_id],
		[project_id]
	) 
	REFERENCES [dbo].[catalogInstanceNames] 
	(
		[id],
		[project_id]
	)
)ON [FG_Statistics_Data]
GO

CREATE INDEX [IX_statsSQLServerAgentJobsHistory_InstanceID] ON [dbo].[statsSQLServerAgentJobsHistory]([instance_id], [project_id]) ON [FG_Statistics_Index]
GO
CREATE INDEX [IX_statsSQLServerAgentJobsHistory_ProjecteID] ON [dbo].[statsSQLServerAgentJobsHistory]([project_id]) ON [FG_Statistics_Index]
GO
