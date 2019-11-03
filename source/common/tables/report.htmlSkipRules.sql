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
RAISERROR('Create table: [report].[htmlSkipRules]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (SELECT * FROM sysobjects WHERE id = OBJECT_ID(N'[report].[htmlSkipRules]') AND type in (N'U'))
DROP TABLE [report].[htmlSkipRules]
GO

CREATE TABLE [report].[htmlSkipRules] 
(
	[id]					[smallint] IDENTITY (1, 1)	NOT NULL,
	[module]				[varchar](32)	NOT NULL,
	[rule_id]				[int]			NOT NULL,
	[rule_name]				[sysname]		NOT NULL,
	[skip_value]			[sysname]		NULL,
	[skip_value2]			[sysname]		NULL,
	[active]				[bit]			NOT NULL CONSTRAINT [DF_htmlSkipRules_Active] DEFAULT (1),
	CONSTRAINT [PK_htmlSkipRules] PRIMARY KEY  CLUSTERED 
	(
		[id]
	),
	CONSTRAINT [UK_htmlSkipRules_Name] UNIQUE  NONCLUSTERED 
	(
		[module],
		[rule_id],
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
INSERT	INTO [report].[htmlSkipRules] ([module], [rule_id], [rule_name], [skip_value], [active])
		SELECT 'health-check',         1, 'Instances - Offline', NULL, 0 UNION ALL
		SELECT 'health-check',         2, 'Instances - Online', NULL, 0 UNION ALL
		SELECT 'health-check',         4, 'Databases Status - Issues Detected', NULL, 0 UNION ALL
		SELECT 'health-check',         8, 'Databases Status - Complete Details', NULL, 0 UNION ALL
		SELECT 'health-check',        16, 'SQL Server Agent Jobs - Job Failures', NULL, 0 UNION ALL
		SELECT 'health-check',        32, 'SQL Server Agent Jobs - Permissions errors', NULL, 0 UNION ALL
		SELECT 'health-check',        64, 'SQL Server Agent Jobs - Complete Details', NULL, 0 UNION ALL
		SELECT 'health-check',       128, 'Big Size for System Databases - Issues Detected', NULL, 0 UNION ALL
		SELECT 'health-check',       256, 'Databases Status - Permissions errors', NULL, 0 UNION ALL
		SELECT 'health-check',       512, 'Databases with Auto Close / Shrink - Issues Detected', NULL, 0 UNION ALL
		SELECT 'health-check',      1024, 'Big Size for Database Log files - Issues Detected', NULL, 0 UNION ALL
		SELECT 'health-check',      2048, 'Low Usage of Data Space - Issues Detected', NULL, 0 UNION ALL
		SELECT 'health-check',      4096, 'Log vs. Data - Allocated Size - Issues Detected', NULL, 0 UNION ALL
		SELECT 'health-check',      8192, 'Outdated Backup for Databases - Issues Detected', NULL, 0 UNION ALL
		SELECT 'health-check',     16384, 'Outdated DBCC CHECKDB Databases - Issues Detected', NULL, 0 UNION ALL
		SELECT 'health-check',     32768, 'High Usage of Log Space - Issues Detected', NULL, 0 UNION ALL
		SELECT 'health-check',     65536, 'Disk Space Information - Complete Detais', NULL, 0 UNION ALL
		SELECT 'health-check',    131072, 'Disk Space Information - Permission errors', NULL, 0 UNION ALL
		SELECT 'health-check',    262144, 'Low Free Disk Space - Issues Detected', NULL, 0 UNION ALL
		SELECT 'health-check',    524288, 'Errorlog messages - Permission errors', NULL, 0 UNION ALL
		SELECT 'health-check',   1048576, 'Errorlog messages - Issues Detected', NULL, 0 UNION ALL
		SELECT 'health-check',   2097152, 'Errorlog messages - Complete Details', NULL, 0 UNION ALL
		SELECT 'health-check',   4194304, 'Databases with Fixed File(s) Size - Issues Detected', NULL, 0 UNION ALL
		SELECT 'health-check',   8388608, 'Databases with (Page Verify not CHECKSUM) or (Page Verify is NONE)', NULL, 0 UNION ALL
		SELECT 'health-check',  16777216, 'Frequently Fragmented Indexes (consider lowering the fill-factor)', NULL, 0 UNION ALL
		SELECT 'health-check',  33554432, 'SQL Server Agent Jobs - Long Running SQL Agent Jobs', NULL, 0 UNION ALL
		SELECT 'health-check',  67108864, 'OS Event messages - Permission errors', NULL, 0 UNION ALL
		SELECT 'health-check', 134217728, 'OS Event messages - Complete Details', NULL, 0 UNION ALL
		SELECT 'health-check', 536870912, 'Failed Login Attempts - Issues Detected', NULL, 0 UNION ALL
		SELECT 'health-check',1073741824, 'Databases(s) Growth - Issues Detected', NULL, 0 
GO
