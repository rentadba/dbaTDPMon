-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 13.10.2015
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================

-----------------------------------------------------------------------------------------------------
--
-----------------------------------------------------------------------------------------------------
RAISERROR('Create table: [monitoring].[alertThresholds]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[monitoring].[alertThresholds]') AND type in (N'U'))
DROP TABLE [monitoring].[alertThresholds]
GO
CREATE TABLE [monitoring].[alertThresholds]
(
	[id]										[int] IDENTITY (1, 1)NOT NULL,
	[category]									[varchar](32)		NOT NULL,
	[alert_name]								[sysname]			NULL,
	[operator]									[varchar](8)		NULL,
	[warning_limit]								[numeric](18,3)		NOT NULL,
	[critical_limit]								[numeric](18,3)	NOT NULL,
	[is_warning_limit_enabled]					[bit]				NOT NULL CONSTRAINT [DF_alertThresholds_IsWarningLimitEnabled] DEFAULT (1),
	[is_critical_limit_enabled]					[bit]				NOT NULL CONSTRAINT [DF_alertThresholds_IsCriticalLimitEnabled] DEFAULT (1),
	CONSTRAINT [PK_alertThresholds] PRIMARY KEY  CLUSTERED 
	(
		[id]
	)  ON [PRIMARY],
	CONSTRAINT [UK_alertThresholds_AlertName] UNIQUE
		(
			[category]
		  , [alert_name]
		) 
) ON [PRIMARY]
GO

SET NOCOUNT ON
-----------------------------------------------------------------------------------------------------
RAISERROR('		...insert default data', 10, 1) WITH NOWAIT
GO
INSERT	INTO [monitoring].[alertThresholds] ([category], [alert_name], [operator], [warning_limit], [critical_limit])
		SELECT 'disk-space', 'Logical Disk: Free Disk Space (%)', '<',     8.0,    5.0 UNION ALL
		SELECT 'disk-space', 'Logical Disk: Free Disk Space (MB)', '<', 3000.0, 2048.0  UNION ALL
		SELECT 'replication', 'Replication Latency', '>', 15.0, 20.0 UNION ALL
		SELECT 'performance', 'Running Transaction Elapsed Time (sec)', '>', 1800, 3600.0 UNION ALL
		SELECT 'performance', 'Uncommitted Transaction Elapsed Time (sec)', '>', 900, 1800.0 UNION ALL
		SELECT 'performance', 'Blocking Transaction Elapsed Time (sec)', '>', 600, 900.0
GO
