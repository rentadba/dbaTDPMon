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
	[skip_value2]			[sysname]		NULL,
	[active]				[bit]			NOT NULL CONSTRAINT [DF_alertSkipRules_Active] DEFAULT (1),
	CONSTRAINT [PK_reportHTMLSkipRules] PRIMARY KEY  CLUSTERED 
	(
		[id]
	) ON [PRIMARY],
	CONSTRAINT [UK_reportHTMLSkipRules_Name] UNIQUE  NONCLUSTERED 
	(
		[category],
		[alert_name],
		[skip_value],
		[skip_value2]
	) ON [PRIMARY]
)  ON [PRIMARY]
GO

-----------------------------------------------------------------------------------------------------
RAISERROR('		...insert default data', 10, 1) WITH NOWAIT
GO
SET NOCOUNT ON
GO
INSERT	INTO [monitoring].[alertSkipRules] ([category], [alert_name], [skip_value], [active])
		SELECT 'disk-space', 'Logical Disk: Free Disk Space (%)', NULL, 0 UNION ALL
		SELECT 'disk-space', 'Logical Disk: Free Disk Space (MB)', NULL, 0
GO
