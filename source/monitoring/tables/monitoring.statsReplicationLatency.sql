-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 23.11.2015
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================

-----------------------------------------------------------------------------------------------------
--
-----------------------------------------------------------------------------------------------------
RAISERROR('Create table: [monitoring].[statsReplicationLatency]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[monitoring].[statsReplicationLatency]') AND type in (N'U'))
DROP TABLE [monitoring].[statsReplicationLatency]
GO
CREATE TABLE [monitoring].[statsReplicationLatency]
(
	[id]						[int] IDENTITY (1, 1) NOT NULL,
	[project_id]				[smallint]	NOT NULL,
	[distributor_server]		[sysname]	NOT NULL,
	[publication_name]			[sysname]	NOT NULL,
	[publication_type]			[int]		NOT NULL,
	[publisher_server]			[sysname]	NOT NULL,
	[publisher_db]				[sysname]	NOT NULL,
	[subscriber_server]			[sysname]	NOT NULL,
	[subscriber_db]				[sysname]	NOT NULL,
	[subscription_type]			[int]		NOT NULL,
	[subscription_status]		[tinyint]	NOT NULL,
	[subscription_articles]		[int]		NULL,
	[distributor_latency]		[int]		NULL,
	[subscriber_latency]		[int]		NULL,
	[overall_latency]			[int]		NULL,
	[event_date_utc]			[datetime]	NOT NULL CONSTRAINT [DF_monitoring_statsReplicationLatency_EventDateUTC] DEFAULT(GETUTCDATE()),
	[state]						[tinyint]	NOT NULL CONSTRAINT [DF_monitoring_statsReplicationLatency_State] DEFAULT (0),
	CONSTRAINT [PK_statsReplicationLatency] PRIMARY KEY CLUSTERED 
	(
		[id]
	),
	CONSTRAINT [FK_statsReplicationLatency_catalogProjects] FOREIGN KEY 
	(
		[project_id]
	) 
	REFERENCES [dbo].[catalogProjects] 
	(
		[id]
	),
)
GO

CREATE INDEX [IX_statsReplicationLatency_ProjectID] ON [monitoring].[statsReplicationLatency] ([project_id])
GO
CREATE INDEX [IX_statsReplicationLatency_PublicationName] ON [monitoring].[statsReplicationLatency]([publication_name], [publisher_server], [publisher_db])
GO
