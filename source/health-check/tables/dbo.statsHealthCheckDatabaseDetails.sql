-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 35.12.2014
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================

-----------------------------------------------------------------------------------------------------
--Health Check: database statistics & details
-----------------------------------------------------------------------------------------------------
RAISERROR('Create table: [dbo].[statsHealthCheckDatabaseDetails]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[statsHealthCheckDatabaseDetails]') AND type in (N'U'))
DROP TABLE [dbo].[statsHealthCheckDatabaseDetails]
GO
CREATE TABLE [dbo].[statsHealthCheckDatabaseDetails]
(
	[id]						[int]	 IDENTITY (1, 1)	NOT NULL,
	[catalog_database_id]		[smallint]		NOT NULL,
	[instance_id]				[smallint]		NOT NULL,
	[recovery_model]			[tinyint]		NULL,
	[page_verify_option]		[tinyint]		NULL,
	[compatibility_level]		[tinyint]		NULL,
	[data_size_mb]				[numeric](20,3)	NULL,
	[data_space_used_percent]	[numeric](6,2)	NULL,
	[log_size_mb]				[numeric](20,3)	NULL,
	[log_space_used_percent]	[numeric](6,2)	NULL,
	[is_auto_close]				[bit]			NULL,
	[is_auto_shrink]			[bit]			NULL,
	[physical_drives]			[sysname]		NULL,
	[last_backup_time]			[datetime]		NULL,
	[last_dbcc checkdb_time]	[datetime]		NULL,
	[is_growth_limited]			[bit]			NULL,
	[event_date_utc]			[datetime]		NOT NULL,
	CONSTRAINT [PK_statsHealthCheckDatabaseDetails] PRIMARY KEY  CLUSTERED 
	(
		[id],
		[catalog_database_id]
	) ON [FG_Statistics_Data],
	CONSTRAINT [FK_statsHealthCheckDatabaseDetails_catalogDatabaseNames] FOREIGN KEY 
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

CREATE INDEX [IX_statsHealthCheckDatabaseDetails_CatalogDatabaseID] ON [dbo].[statsHealthCheckDatabaseDetails]( [catalog_database_id], [instance_id]) ON [FG_Statistics_Index]
GO
CREATE INDEX [IX_statsHealthCheckDatabaseDetails_InstanceID] ON [dbo].[statsHealthCheckDatabaseDetails] ([instance_id]) ON [FG_Statistics_Index]
GO
