-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 15.12.2014
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================

-----------------------------------------------------------------------------------------------------
--Health Check: disk space information
-----------------------------------------------------------------------------------------------------
RAISERROR('Create table: [health-check].[statsDiskSpaceInfo]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[health-check].[statsDiskSpaceInfo]') AND type in (N'U'))
DROP TABLE [health-check].[statsDiskSpaceInfo]
GO
CREATE TABLE [health-check].[statsDiskSpaceInfo]
(
	[id]					[int]	 IDENTITY (1, 1)	NOT NULL,
	[instance_id]			[smallint]		NOT NULL,
	[project_id]			[smallint]		NOT NULL,
	[event_date_utc]		[datetime]		NOT NULL,
	[logical_drive]			[char](1)			NULL,
	[volume_mount_point]	[nvarchar](512)		NULL,
	[total_size_mb]			[numeric](18,3)		NULL,
	[available_space_mb]	[numeric](18,3)		NULL,
	[block_size]			[int]				NULL,
	[percent_available]		[numeric](6,2)		NULL
	CONSTRAINT [PK_statsDiskSpaceInfo] PRIMARY KEY  CLUSTERED 
	(
		[id],
		[instance_id]
	),
	CONSTRAINT [FK_statsDiskSpaceInfo_catalogProjects] FOREIGN KEY 
	(
		[project_id]
	) 
	REFERENCES [dbo].[catalogProjects] 
	(
		[id]
	),
	CONSTRAINT [FK_statsDiskSpaceInfo_catalogInstanceNames] FOREIGN KEY 
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

CREATE INDEX [IX_statsDiskSpaceInfo_InstanceID] ON [health-check].[statsDiskSpaceInfo]([instance_id], [project_id])
GO
CREATE INDEX [IX_statsDiskSpaceInfo_ProjecteID] ON [health-check].[statsDiskSpaceInfo]([project_id])
GO
