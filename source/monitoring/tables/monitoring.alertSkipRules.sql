-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 30.09.2015
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================

-----------------------------------------------------------------------------------------------------
--HTML reports rules / checks and instances/machines to be skipped
-----------------------------------------------------------------------------------------------------
RAISERROR('Create table: [monitoring].[alertSkipRules]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (SELECT * FROM sysobjects WHERE id = OBJECT_ID(N'[monitoring].[alertSkipRules]') AND type in (N'U'))
DROP TABLE [monitoring].[alertSkipRules]
GO

CREATE TABLE [monitoring].[alertSkipRules] 
(
	[id]					[smallint] IDENTITY (1, 1)	NOT NULL,
	[category]				[varchar](32)	NOT NULL,
	[alert_name]			[sysname]		NULL,
	[skip_value]			[sysname]		NULL,
	[skip_value2]			[nvarchar](512)	NULL,
	[active]				[bit]			NOT NULL CONSTRAINT [DF_alertSkipRules_Active] DEFAULT (1),
	CONSTRAINT [PK_alertSkipRules] PRIMARY KEY  CLUSTERED 
	(
		[id]
	),
	CONSTRAINT [UK_alertSkipRules_Name] UNIQUE  NONCLUSTERED 
	(
		[category],
		[alert_name],
		[skip_value],
		[skip_value2]
	)
)
GO

-----------------------------------------------------------------------------------------------------
RAISERROR('		...insert default data', 10, 1) WITH NOWAIT
GO
SET NOCOUNT ON
GO
INSERT	INTO [monitoring].[alertSkipRules] ([category], [alert_name], [skip_value], [skip_value2], [active])
		SELECT 'disk-space', 'Logical Disk: Free Disk Space (%)', NULL, NULL, 0 UNION ALL
		SELECT 'disk-space', 'Logical Disk: Free Disk Space (MB)', NULL, NULL, 0 UNION ALL
		SELECT 'replication', 'subscription marked inactive', '[PublisherServer].[PublishedDB](PublicationName)', '[SubscriberServer].[SubscriberDB]', 0 UNION ALL
		SELECT 'replication', 'subscription not active', '[PublisherServer].[PublishedDB](PublicationName)', '[SubscriberServer].[SubscriberDB]', 0 UNION ALL
		SELECT 'replication', 'replication latency', '[PublisherServer].[PublishedDB](PublicationName)', '[SubscriberServer].[SubscriberDB]', 0 UNION ALL
		SELECT 'performance', 'Running Transaction Elapsed Time (sec)', 'InstanceName', NULL, 0 UNION ALL
		SELECT 'performance', 'Uncommitted Transaction Elapsed Time (sec)', 'InstanceName', NULL, 0 UNION ALL
		SELECT 'performance', 'Blocking Transaction Elapsed Time (sec)', 'InstanceName', NULL, 0 UNION ALL
		SELECT 'performance', 'Active Request/Session Elapsed Time (sec)', 'InstanceName', NULL, 0 UNION ALL
		SELECT 'performance', 'tempdb: space used by a single session', 'InstanceName', NULL, 0
GO


