-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 21.03.2017
-- Module			 : Database Analysis & Performance health-check
-- ============================================================================

-----------------------------------------------------------------------------------------------------
--Health Check: database statistics & details
-----------------------------------------------------------------------------------------------------
RAISERROR('Create table: [health-check].[statsDatabaseUsageHistory]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[health-check].[statsDatabaseUsageHistory]') AND type in (N'U'))
DROP TABLE [health-check].[statsDatabaseUsageHistory]
GO
CREATE TABLE [health-check].[statsDatabaseUsageHistory]
(
	[id]						[int]	 IDENTITY (1, 1)	NOT NULL,
	[catalog_database_id]		[int]			NOT NULL,
	[instance_id]				[smallint]		NOT NULL,
	[data_size_mb]				[numeric](20,3)	NULL,
	[data_space_used_percent]	[numeric](6,2)	NULL,
	[log_size_mb]				[numeric](20,3)	NULL,
	[log_space_used_percent]	[numeric](6,2)	NULL,
	[volume_mount_point]		[nvarchar](512)	NULL,
	[event_date_utc]			[datetime]		NOT NULL,
	CONSTRAINT [PK_statsDatabaseUsageHistory] PRIMARY KEY  CLUSTERED 
	(
		[id],
		[catalog_database_id]
	) ON [FG_Statistics_Data],
	CONSTRAINT [FK_statsDatabaseUsageHistory_catalogDatabaseNames] FOREIGN KEY 
	(
		  [catalog_database_id]
		, [instance_id]
	) 
	REFERENCES [dbo].[catalogDatabaseNames] 
	(
		  [id]
		, [instance_id]
	)
)ON [FG_Statistics_Data]
GO

CREATE INDEX [IX_statsDatabaseUsageHistory_CatalogDatabaseID] ON [health-check].[statsDatabaseUsageHistory]( [catalog_database_id], [instance_id]) ON [FG_Statistics_Index]
GO
CREATE INDEX [IX_statsDatabaseUsageHistory_InstanceID] ON [health-check].[statsDatabaseUsageHistory] ([instance_id]) ON [FG_Statistics_Index]
GO
