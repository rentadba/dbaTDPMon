-- ============================================================================
-- Copyright (c) 2004-2016 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 11.01.2016
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================

-----------------------------------------------------------------------------------------------------
--Health Check: disk space information
-----------------------------------------------------------------------------------------------------
RAISERROR('Create table: [monitoring].[statsTransactionsStatus]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[monitoring].[statsTransactionsStatus]') AND type in (N'U'))
DROP TABLE [monitoring].[statsTransactionsStatus]
GO
CREATE TABLE [monitoring].[statsTransactionsStatus]
(
	[id]								[int]	 IDENTITY (1, 1)	NOT NULL,
	[instance_id]						[smallint]		NOT NULL,
	[project_id]						[smallint]		NOT NULL,
	[event_date_utc]					[datetime]		NOT NULL,
	[database_name]						[sysname],
	[session_id]						[smallint],
	[request_id]						[int],
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
	[sql_text]							[nvarchar](max)
	CONSTRAINT [PK_statsTransactionsStatus] PRIMARY KEY  CLUSTERED 
	(
		[id],
		[instance_id]
	),
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
)
GO

CREATE INDEX [IX_statsTransactionsStatus_InstanceID] ON [monitoring].[statsTransactionsStatus]([instance_id], [project_id])
GO
CREATE INDEX [IX_statsTransactionsStatus_ProjecteID] ON [monitoring].[statsTransactionsStatus]([project_id])
GO
