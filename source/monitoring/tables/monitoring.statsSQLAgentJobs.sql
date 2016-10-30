-- ============================================================================
-- Copyright (c) 2004-2016 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 03.02.2016
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================

-----------------------------------------------------------------------------------------------------
--
-----------------------------------------------------------------------------------------------------
RAISERROR('Create table: [monitoring].[statsSQLAgentJobs]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[monitoring].[statsSQLAgentJobs]') AND type in (N'U'))
DROP TABLE [monitoring].[statsSQLAgentJobs]
GO
CREATE TABLE [monitoring].[statsSQLAgentJobs]
(
	[id]								[int]	 IDENTITY (1, 1)	NOT NULL,
	[instance_id]						[smallint]		NOT NULL,
	[project_id]						[smallint]		NOT NULL,
	[event_date_utc]					[datetime]		NOT NULL,
	[job_name]							[sysname]		NOT NULL,
	[job_completion_status]				[tinyint],
	[last_completion_time]				[datetime],
	[last_completion_time_utc]			[datetime],
	[local_server_date_utc]				[datetime],
	CONSTRAINT [PK_statsSQLAgentJobs] PRIMARY KEY  CLUSTERED 
	(
		[id],
		[instance_id]
	) ON [FG_Statistics_Data],
	CONSTRAINT [FK_statsSQLAgentJobs_catalogProjects] FOREIGN KEY 
	(
		[project_id]
	) 
	REFERENCES [dbo].[catalogProjects] 
	(
		[id]
	),
	CONSTRAINT [FK_statsSQLAgentJobs_catalogInstanceNames] FOREIGN KEY 
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

CREATE INDEX [IX_statsSQLAgentJobs_InstanceID] ON [monitoring].[statsSQLAgentJobs]([instance_id], [project_id]) INCLUDE ([last_completion_time]) ON [FG_Statistics_Index]
GO
CREATE INDEX [IX_statsSQLAgentJobs_ProjecteID] ON [monitoring].[statsSQLAgentJobs]([project_id]) ON [FG_Statistics_Index]
GO
