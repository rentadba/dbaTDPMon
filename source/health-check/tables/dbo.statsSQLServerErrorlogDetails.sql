-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 29.05.2015
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================

-----------------------------------------------------------------------------------------------------
--log for SQL Server Agent job statuses
-----------------------------------------------------------------------------------------------------
RAISERROR('Create table: [dbo].[statsSQLServerErrorlogDetails]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[statsSQLServerErrorlogDetails]') AND type in (N'U'))
DROP TABLE [dbo].[statsSQLServerErrorlogDetails]
GO
CREATE TABLE [dbo].[statsSQLServerErrorlogDetails]
(
	[id]					[int]	 IDENTITY (1, 1)	NOT NULL,
	[instance_id]			[smallint]		NOT NULL,
	[project_id]			[smallint]		NOT NULL,
	[event_date_utc]		[datetime]		NOT NULL,
	[log_date]				[datetime]		NULL,
	[process_info]			[sysname]		NULL,
	[text]					[varchar](max)	NULL,
	CONSTRAINT [PK_statsSQLServerErrorlogDetails] PRIMARY KEY  CLUSTERED 
	(
		[id],
		[instance_id]
	) ON [FG_Statistics_Data],
	CONSTRAINT [FK_statsSQLServerErrorlogDetails_catalogProjects] FOREIGN KEY 
	(
		[project_id]
	) 
	REFERENCES [dbo].[catalogProjects] 
	(
		[id]
	),
	CONSTRAINT [FK_statsSQLServerErrorlogDetails_catalogInstanceNames] FOREIGN KEY 
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

CREATE INDEX [IX_statsSQLServerErrorlogDetails_ProjectID] ON [dbo].[statsSQLServerErrorlogDetails] ([project_id]) ON [FG_Statistics_Index]
GO
CREATE INDEX [IX_statsSQLServerErrorlogDetails_InstanceID] ON [dbo].[statsSQLServerErrorlogDetails] ([instance_id], [project_id], [log_date]) ON [FG_Statistics_Index]
GO
CREATE INDEX [IX_statsSQLServerErrorlogDetails_LogDate] ON [dbo].[statsSQLServerErrorlogDetails]([log_date], [instance_id], [project_id]) ON [FG_Statistics_Index]
GO
