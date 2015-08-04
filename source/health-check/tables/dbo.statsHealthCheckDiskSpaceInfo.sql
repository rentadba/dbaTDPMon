-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 35.12.2014
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================

-----------------------------------------------------------------------------------------------------
--Health Check: disk space information
-----------------------------------------------------------------------------------------------------
RAISERROR('Create table: [dbo].[statsHealthCheckDiskSpaceInfo]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[statsHealthCheckDiskSpaceInfo]') AND type in (N'U'))
DROP TABLE [dbo].[statsHealthCheckDiskSpaceInfo]
GO
CREATE TABLE [dbo].[statsHealthCheckDiskSpaceInfo]
(
	[id]					[int]	 IDENTITY (1, 1)	NOT NULL,
	[instance_id]			[smallint]		NOT NULL,
	[project_id]			[smallint]		NOT NULL,
	[event_date_utc]		[datetime]		NOT NULL,
	[logical_drive]			[char](1)			NULL,
	[volume_mount_point]	[nvarchar](512)		NULL,
	[total_size_mb]			[numeric](18,3)		NULL,
	[available_space_mb]	[numeric](18,3)		NULL,
	[percent_available]		[numeric](6,2)		NULL
	CONSTRAINT [PK_statsHealthCheckDiskSpaceInfo] PRIMARY KEY  CLUSTERED 
	(
		[id],
		[instance_id]
	) ON [FG_Statistics_Data],
	CONSTRAINT [FK_statsHealthCheckDiskSpaceInfo_catalogProjects] FOREIGN KEY 
	(
		[project_id]
	) 
	REFERENCES [dbo].[catalogProjects] 
	(
		[id]
	),
	CONSTRAINT [FK_statsHealthCheckDiskSpaceInfo_catalogInstanceNames] FOREIGN KEY 
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

CREATE INDEX [IX_statsHealthCheckDiskSpaceInfo_InstanceID] ON [dbo].[statsHealthCheckDiskSpaceInfo]([instance_id], [project_id]) ON [FG_Statistics_Index]
GO
CREATE INDEX [IX_statsHealthCheckDiskSpaceInfo_ProjecteID] ON [dbo].[statsHealthCheckDiskSpaceInfo]([project_id]) ON [FG_Statistics_Index]
GO
