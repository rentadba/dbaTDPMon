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
RAISERROR('Create table: [health-check].[statsSQLAgentJobsHistory]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[health-check].[statsSQLAgentJobsHistory]') AND type in (N'U'))
DROP TABLE [health-check].[statsSQLAgentJobsHistory]
GO
CREATE TABLE [health-check].[statsSQLAgentJobsHistory]
(
	[id]					[int]	 IDENTITY (1, 1)	NOT NULL,
	[instance_id]			[smallint]		NOT NULL,
	[project_id]			[smallint]		NOT NULL,
	[event_date_utc]		[datetime]		NOT NULL,
	[job_name]				[sysname]		NOT NULL,
	[last_execution_status] [int]			NOT NULL,
	[last_execution_date]	[varchar](10)	NULL, 
	[last_execution_time]	[varchar](8)	NULL,
	[last_execution_utc]	[datetime]		NULL,
	[running_time_sec]		[bigint]		NULL,
	[message]				[varchar](max)	NULL, 
	CONSTRAINT [PK_statsSQLAgentJobsHistory] PRIMARY KEY  CLUSTERED 
	(
		[id],
		[instance_id]
	),
	CONSTRAINT [FK_statsSQLAgentJobsHistory_catalogProjects] FOREIGN KEY 
	(
		[project_id]
	) 
	REFERENCES [dbo].[catalogProjects] 
	(
		[id]
	),
	CONSTRAINT [FK_statsSQLAgentJobsHistory_catalogInstanceNames] FOREIGN KEY 
	(
		[instance_id],
		[project_id]
	) 
	REFERENCES [dbo].[catalogInstanceNames] 
	(
		[id],
		[project_id]
	)
)
GO

CREATE INDEX [IX_statsSQLAgentJobsHistory_InstanceID] ON [health-check].[statsSQLAgentJobsHistory]([instance_id], [project_id])
GO
CREATE INDEX [IX_statsSQLAgentJobsHistory_ProjecteID] ON [health-check].[statsSQLAgentJobsHistory]([project_id])
GO
