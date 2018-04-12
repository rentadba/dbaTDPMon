SET NOCOUNT ON
/*
*-----------------------------------------------------------------------------*
* dbaTDPMon (Troubleshoot Database Performance / Monitoring)                  *
* https://github.com/rentadba/dbaTDPMon, under GNU (GPLv3) licence model      *
*-----------------------------------------------------------------------------*
* Patch script: from version 2016.11 to 2017.4 (2017.03.24)					  *
*-----------------------------------------------------------------------------*
*/

/*---------------------------------------------------------------------------------------------------------------------*/
/* patch module: health-check																						   */
/*---------------------------------------------------------------------------------------------------------------------*/
RAISERROR('* Patch: 20170324-patch-upgrade-from-v2016_11-to-v2017_4-hc.sql', 10, 1) WITH NOWAIT

IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[health-check].[statsDatabaseUsageHistory]') AND type in (N'U'))
	begin
		RAISERROR('	Create table: [health-check].[statsDatabaseUsageHistory]', 10, 1) WITH NOWAIT

		CREATE TABLE [health-check].[statsDatabaseUsageHistory]
		(
			[id]						[int]	 IDENTITY (1, 1)	NOT NULL,
			[catalog_database_id]		[smallint]		NOT NULL,
			[instance_id]				[smallint]		NOT NULL,
			[data_size_mb]				[numeric](20,3)	NULL,
			[data_space_used_percent]	[numeric](6,2)	NULL,
			[log_size_mb]				[numeric](20,3)	NULL,
			[log_space_used_percent]	[numeric](6,2)	NULL,
			[physical_drives]			[sysname]		NULL,
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
		)ON [FG_Statistics_Data];

		CREATE INDEX [IX_statsDatabaseUsageHistory_CatalogDatabaseID] ON [health-check].[statsDatabaseUsageHistory]( [catalog_database_id], [instance_id]) ON [FG_Statistics_Index];
		CREATE INDEX [IX_statsDatabaseUsageHistory_InstanceID] ON [health-check].[statsDatabaseUsageHistory] ([instance_id]) ON [FG_Statistics_Index];
	end
GO

IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[health-check].[statsErrorlogDetails]') AND type in (N'U'))
	begin
		RAISERROR('	Create table: [health-check].[statsErrorlogDetails]', 10, 1) WITH NOWAIT

		CREATE TABLE [health-check].[statsErrorlogDetails]
		(
			[id]					[int]	 IDENTITY (1, 1)	NOT NULL,
			[instance_id]			[smallint]		NOT NULL,
			[project_id]			[smallint]		NOT NULL,
			[event_date_utc]		[datetime]		NOT NULL,
			[log_date]				[datetime]		NULL,
			[process_info]			[sysname]		NULL,
			[text]					[varchar](max)	NULL,
			CONSTRAINT [PK_statsErrorlogDetails] PRIMARY KEY  CLUSTERED 
			(
				[id],
				[instance_id]
			) ON [FG_Statistics_Data],
			CONSTRAINT [FK_statsErrorlogDetails_catalogProjects] FOREIGN KEY 
			(
				[project_id]
			) 
			REFERENCES [dbo].[catalogProjects] 
			(
				[id]
			),
			CONSTRAINT [FK_statsErrorlogDetails_catalogInstanceNames] FOREIGN KEY 
			(
				[instance_id],
				[project_id]
			) 
			REFERENCES [dbo].[catalogInstanceNames] 
			(
				[id],
				[project_id]
			)
		)ON [FG_Statistics_Data];

		CREATE INDEX [IX_statsErrorlogDetails_ProjectID] ON [health-check].[statsErrorlogDetails] ([project_id]) ON [FG_Statistics_Index];
		CREATE INDEX [IX_statsErrorlogDetails_InstanceID] ON [health-check].[statsErrorlogDetails] ([instance_id], [project_id], [log_date]) ON [FG_Statistics_Index];
		CREATE INDEX [IX_statsErrorlogDetails_LogDate] ON [health-check].[statsErrorlogDetails]([log_date], [instance_id], [project_id]) ON [FG_Statistics_Index];
	end
GO

IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[health-check].[statsSQLAgentJobsHistory]') AND type in (N'U'))
	begin
		RAISERROR('	Create table: [health-check].[statsSQLAgentJobsHistory]', 10, 1) WITH NOWAIT

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
			[running_time_sec]		[bigint]		NULL,
			[message]				[varchar](max)	NULL, 
			CONSTRAINT [PK_statsSQLAgentJobsHistory] PRIMARY KEY  CLUSTERED 
			(
				[id],
				[instance_id]
			) ON [FG_Statistics_Data],
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
		)ON [FG_Statistics_Data];

		CREATE INDEX [IX_statsSQLAgentJobsHistory_InstanceID] ON [health-check].[statsSQLAgentJobsHistory]([instance_id], [project_id]) ON [FG_Statistics_Index];
		CREATE INDEX [IX_statsSQLAgentJobsHistory_ProjecteID] ON [health-check].[statsSQLAgentJobsHistory]([project_id]) ON [FG_Statistics_Index];
	end
GO

IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[health-check].[statsSQLServerErrorlogDetails]') AND type in (N'U'))
	begin
		RAISERROR('	Save records: [health-check].[statsSQLServerErrorlogDetails]', 10, 1) WITH NOWAIT

		SET IDENTITY_INSERT [health-check].[statsErrorlogDetails] ON;
		INSERT	INTO [health-check].[statsErrorlogDetails]([id], [instance_id], [project_id], [event_date_utc], [log_date], [process_info], [text])
				SELECT [id], [instance_id], [project_id], [event_date_utc], [log_date], [process_info], [text]
				FROM [health-check].[statsSQLServerErrorlogDetails];
		SET IDENTITY_INSERT [health-check].[statsErrorlogDetails] OFF;

		RAISERROR('	Drop table: [health-check].[statsSQLServerErrorlogDetails]', 10, 1) WITH NOWAIT
		DROP TABLE [health-check].[statsSQLServerErrorlogDetails]
	end
GO

IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[health-check].[statsSQLServerAgentJobsHistory]') AND type in (N'U'))
	begin
		RAISERROR('	Save records: [health-check].[statsSQLServerAgentJobsHistory]', 10, 1) WITH NOWAIT

		SET IDENTITY_INSERT [health-check].[statsSQLAgentJobsHistory] ON;
		INSERT	INTO [health-check].[statsSQLAgentJobsHistory]([id], [instance_id], [project_id], [event_date_utc], [job_name], [last_execution_status], [last_execution_date], [last_execution_time], [running_time_sec], [message])
				SELECT [id], [instance_id], [project_id], [event_date_utc], [job_name], [last_execution_status], [last_execution_date], [last_execution_time], [running_time_sec], [message]
				FROM [health-check].[statsSQLServerAgentJobsHistory];
		SET IDENTITY_INSERT [health-check].[statsSQLAgentJobsHistory] OFF;

		RAISERROR('	Drop table: [health-check].[statsSQLServerAgentJobsHistory]', 10, 1) WITH NOWAIT
		DROP TABLE [health-check].[statsSQLServerAgentJobsHistory]
	end
GO

IF  EXISTS (SELECT * FROM sys.views WHERE object_id = OBJECT_ID(N'[health-check].[vw_statsSQLServerAgentJobsHistory]'))
	begin
		RAISERROR('	Drop view : [health-check].[vw_statsSQLServerAgentJobsHistory]', 10, 1) WITH NOWAIT
		DROP VIEW [health-check].[vw_statsSQLServerAgentJobsHistory]
	end
GO

IF  EXISTS (SELECT * FROM sys.views WHERE object_id = OBJECT_ID(N'[health-check].[vw_statsSQLServerErrorlogDetails]'))
	begin
		RAISERROR('	Drop view : [health-check].[vw_statsSQLServerErrorlogDetails]', 10, 1) WITH NOWAIT
		DROP VIEW [health-check].[vw_statsSQLServerErrorlogDetails]
	end
GO
