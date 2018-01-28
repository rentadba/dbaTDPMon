SET NOCOUNT ON
/*
*-----------------------------------------------------------------------------*
* dbaTDPMon (Troubleshoot Database Performance / Monitoring)                  *
* https://github.com/rentadba/dbaTDPMon, under GNU (GPLv3) licence model      *
*-----------------------------------------------------------------------------*
* Patch script: from version 2015.12 to 2016.6 (2016.06.24)					  *
*-----------------------------------------------------------------------------*
*/

/*---------------------------------------------------------------------------------------------------------------------*/
/* patch module: monitoring																							   */
/*---------------------------------------------------------------------------------------------------------------------*/
RAISERROR('* Patch: 20160624-patch-upgrade-from-v2015_12-to-v2016_6-mon.sql', 10, 1) WITH NOWAIT

IF NOT EXISTS(SELECT * FROM [monitoring].[alertThresholds] WHERE [category]='performance' AND [alert_name] = 'Running Transaction Elapsed Time (sec)')
	INSERT	INTO [monitoring].[alertThresholds] ([category], [alert_name], [operator], [warning_limit], [critical_limit])
		SELECT 'performance', 'Running Transaction Elapsed Time (sec)', '>', 1800, 3600
GO

IF NOT EXISTS(SELECT * FROM [monitoring].[alertThresholds] WHERE [category]='performance' AND [alert_name] = 'Uncommitted Transaction Elapsed Time (sec)')
	INSERT	INTO [monitoring].[alertThresholds] ([category], [alert_name], [operator], [warning_limit], [critical_limit])
		SELECT 'performance', 'Uncommitted Transaction Elapsed Time (sec)', '>', 900, 1800
GO

IF NOT EXISTS(SELECT * FROM [monitoring].[alertThresholds] WHERE [category]='performance' AND [alert_name] = 'Blocking Transaction Elapsed Time (sec)')
	INSERT	INTO [monitoring].[alertThresholds] ([category], [alert_name], [operator], [warning_limit], [critical_limit])
		SELECT 'performance', 'Blocking Transaction Elapsed Time (sec)', '>', 600, 900
GO

IF NOT EXISTS(SELECT * FROM [monitoring].[alertThresholds] WHERE [category]='performance' AND [alert_name] = 'tempdb: space used by a single session')
	INSERT	INTO [monitoring].[alertThresholds] ([category], [alert_name], [operator], [warning_limit], [critical_limit])
		SELECT 'performance', 'tempdb: space used by a single session', '>', 8192, 16384
GO

IF NOT EXISTS(SELECT * FROM [monitoring].[alertSkipRules] WHERE [category]='disk-space' AND [alert_name]='Logical Disk: Free Disk Space (%)')
INSERT	INTO [monitoring].[alertSkipRules] ([category], [alert_name], [skip_value], [skip_value2], [active])
		SELECT 'disk-space', 'Logical Disk: Free Disk Space (%)', NULL, NULL, 0

IF NOT EXISTS(SELECT * FROM [monitoring].[alertSkipRules] WHERE [category]='disk-space' AND [alert_name]='Logical Disk: Free Disk Space (MB)')
INSERT	INTO [monitoring].[alertSkipRules] ([category], [alert_name], [skip_value], [skip_value2], [active])
		SELECT 'disk-space', 'Logical Disk: Free Disk Space (MB)', NULL, NULL, 0

IF NOT EXISTS(SELECT * FROM [monitoring].[alertSkipRules] WHERE [category]='replication' AND [alert_name]='subscription marked inactive')
INSERT	INTO [monitoring].[alertSkipRules] ([category], [alert_name], [skip_value], [skip_value2], [active])
		SELECT 'replication', 'subscription marked inactive', '[PublisherServer].[PublishedDB](PublicationName)', '[SubscriberServer].[SubscriberDB]', 0

IF NOT EXISTS(SELECT * FROM [monitoring].[alertSkipRules] WHERE [category]='replication' AND [alert_name]='subscription not active')
INSERT	INTO [monitoring].[alertSkipRules] ([category], [alert_name], [skip_value], [skip_value2], [active])
		SELECT 'replication', 'subscription not active', '[PublisherServer].[PublishedDB](PublicationName)', '[SubscriberServer].[SubscriberDB]', 0

IF NOT EXISTS(SELECT * FROM [monitoring].[alertSkipRules] WHERE [category]='replication' AND [alert_name]='replication latency')
INSERT	INTO [monitoring].[alertSkipRules] ([category], [alert_name], [skip_value], [skip_value2], [active])
		SELECT 'replication', 'replication latency', '[PublisherServer].[PublishedDB](PublicationName)', '[SubscriberServer].[SubscriberDB]', 0

IF NOT EXISTS(SELECT * FROM [monitoring].[alertSkipRules] WHERE [category]='performance' AND [alert_name]='Running Transaction Elapsed Time (sec)')
INSERT	INTO [monitoring].[alertSkipRules] ([category], [alert_name], [skip_value], [skip_value2], [active])
		SELECT 'performance', 'Running Transaction Elapsed Time (sec)', 'InstanceName', NULL, 0

IF NOT EXISTS(SELECT * FROM [monitoring].[alertSkipRules] WHERE [category]='performance' AND [alert_name]='Uncommitted Transaction Elapsed Time (sec)')
INSERT	INTO [monitoring].[alertSkipRules] ([category], [alert_name], [skip_value], [skip_value2], [active])
		SELECT 'performance', 'Uncommitted Transaction Elapsed Time (sec)', 'InstanceName', NULL, 0

IF NOT EXISTS(SELECT * FROM [monitoring].[alertSkipRules] WHERE [category]='performance' AND [alert_name]='Blocking Transaction Elapsed Time (sec)')
INSERT	INTO [monitoring].[alertSkipRules] ([category], [alert_name], [skip_value], [skip_value2], [active])
		SELECT 'performance', 'Blocking Transaction Elapsed Time (sec)', 'InstanceName', NULL, 0

IF NOT EXISTS(SELECT * FROM [monitoring].[alertSkipRules] WHERE [category]='performance' AND [alert_name]='tempdb: space used by a single session')
INSERT	INTO [monitoring].[alertSkipRules] ([category], [alert_name], [skip_value], [skip_value2], [active])
		SELECT 'performance', 'tempdb: space used by a single session', 'InstanceName', NULL, 0
GO



IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[monitoring].[statsSQLAgentJobs]') AND type in (N'U'))
	begin
		RAISERROR('	Create table: [monitoring].[statsSQLAgentJobs]', 10, 1) WITH NOWAIT

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
		)ON [FG_Statistics_Data];

		CREATE INDEX [IX_statsSQLAgentJobs_InstanceID] ON [monitoring].[statsSQLAgentJobs]([instance_id], [project_id]) INCLUDE ([last_completion_time]) ON [FG_Statistics_Index];
		CREATE INDEX [IX_statsSQLAgentJobs_ProjecteID] ON [monitoring].[statsSQLAgentJobs]([project_id]) ON [FG_Statistics_Index];
	end
GO		


IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[monitoring].[statsTransactionsStatus]') AND type in (N'U'))
	begin
		RAISERROR('	Create table: [monitoring].[statsTransactionsStatus]', 10, 1) WITH NOWAIT

		CREATE TABLE [monitoring].[statsTransactionsStatus]
		(
			[id]								[int]	 IDENTITY (1, 1)	NOT NULL,
			[instance_id]						[smallint]		NOT NULL,
			[project_id]						[smallint]		NOT NULL,
			[event_date_utc]					[datetime]		NOT NULL,
			[database_name]						[sysname],
			[session_id]						[smallint],
			[transaction_begin_time]			[datetime],
			[host_name]							[sysname],
			[program_name]						[sysname],
			[login_name]						[sysname],
			[last_request_elapsed_time_sec]		[int],
			[transaction_elapsed_time_sec]		[int],
			[sessions_blocked]					[smallint],
			[is_session_blocked]				[bit],
			[sql_handle]						[varbinary](64),
			[request_completed]					[bit],
			[wait_duration_sec]					[int],
			[wait_type]							[nvarchar](60),
			[tempdb_space_used_mb]				[int],
			CONSTRAINT [PK_statsTransactionsStatus] PRIMARY KEY  CLUSTERED 
			(
				[id],
				[instance_id]
			) ON [FG_Statistics_Data],
			CONSTRAINT [FK_statsTransactionsStatus_catalogProjects] FOREIGN KEY 
			(
				[project_id]
			) 
			REFERENCES [dbo].[catalogProjects] 
			(
				[id]
			),
			CONSTRAINT [FK_statsTransactionsStatus_catalogInstanceNames] FOREIGN KEY 
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

		CREATE INDEX [IX_statsTransactionsStatus_InstanceID] ON [monitoring].[statsTransactionsStatus]([instance_id], [project_id]) ON [FG_Statistics_Index];
		CREATE INDEX [IX_statsTransactionsStatus_ProjecteID] ON [monitoring].[statsTransactionsStatus]([project_id]) ON [FG_Statistics_Index];
	end
GO

IF  EXISTS (
	    SELECT * 
	      FROM sys.objects 
	     WHERE object_id = OBJECT_ID(N'[dbo].[usp_monReplicationPublicationLatency]') 
	       AND type in (N'P', N'PC'))
	begin
		RAISERROR('	Drop procedure: [dbo].[usp_monReplicationPublicationLatency]', 10, 1) WITH NOWAIT

		DROP PROCEDURE [dbo].[usp_monReplicationPublicationLatency]
	end
GO

IF EXISTS(SELECT * FROM sys.indexes WHERE [name] = 'IX_statsReplicationLatency_PublisherDB_SubcriptionDB' AND [object_id] = OBJECT_ID('[monitoring].[statsReplicationLatency]'))
	DROP INDEX [IX_statsReplicationLatency_PublisherDB_SubcriptionDB] ON [monitoring].[statsReplicationLatency];
GO
