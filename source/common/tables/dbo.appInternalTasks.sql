-- ============================================================================
-- Copyright (c) 2004-2018 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 11.04.2018
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================

-----------------------------------------------------------------------------------------------------
--
-----------------------------------------------------------------------------------------------------
RAISERROR('Create table: [dbo].[appInternalTasks]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (SELECT * FROM sysobjects WHERE id = OBJECT_ID(N'[dbo].[appInternalTasks]') AND type in (N'U'))
DROP TABLE [dbo].[appInternalTasks]
GO

CREATE TABLE [dbo].[appInternalTasks]
(
	[id]					[bigint]		NOT NULL,
	[descriptor]			[varchar](256)	NOT NULL,
	[task_name]				[varchar](256)	NOT NULL,
	[flg_actions]			[smallint]		NULL,
	CONSTRAINT [PK_appInternalTasks] PRIMARY KEY  CLUSTERED 
	(
		[id]
	) ON [PRIMARY],
	CONSTRAINT [UK_appInternalTasks] UNIQUE
	(
		  [descriptor]
		, [task_name]
	) ON [PRIMARY]
) ON [PRIMARY]
GO

-----------------------------------------------------------------------------------------------------
RAISERROR('		...insert default data', 10, 1) WITH NOWAIT
GO
SET NOCOUNT ON
GO
INSERT	INTO [dbo].[appInternalTasks] ([id], [descriptor], [task_name], [flg_actions])
		SELECT       1, 'dbo.usp_mpDatabaseConsistencyCheck', 'Database Consistency Check', 1 UNION ALL
		SELECT       2, 'dbo.usp_mpDatabaseConsistencyCheck', 'Allocation Consistency Check', 12 UNION ALL
		SELECT       4, 'dbo.usp_mpDatabaseConsistencyCheck', 'Tables Consistency Check', 2 UNION ALL
		SELECT       8, 'dbo.usp_mpDatabaseConsistencyCheck', 'Reference Consistency Check', 16 UNION ALL
		SELECT      16, 'dbo.usp_mpDatabaseConsistencyCheck', 'Perform Correction to Space Usage', 64 UNION ALL
		SELECT      32, 'dbo.usp_mpDatabaseOptimize', 'Rebuild Heap Tables', 16 UNION ALL
		SELECT      64, 'dbo.usp_mpDatabaseOptimize', 'Rebuild or Reorganize Indexes', 3 UNION ALL
		SELECT     128, 'dbo.usp_mpDatabaseOptimize', 'Update Statistics', 8 UNION ALL
		SELECT     256, 'dbo.usp_mpDatabaseShrink', 'Shrink Database (TRUNCATEONLY)', 2 UNION ALL
		SELECT     512, 'dbo.usp_mpDatabaseShrink', 'Shrink Log File', 1 UNION ALL
		SELECT    1024, 'dbo.usp_mpDatabaseBackup', 'User Databases (diff)', 2 UNION ALL
		SELECT    2048, 'dbo.usp_mpDatabaseBackup', 'User Databases (full)' , 1UNION ALL
		SELECT    4096, 'dbo.usp_mpDatabaseBackup', 'System Databases (full)', 1 UNION ALL
		SELECT    8192, 'dbo.usp_mpDatabaseBackup', 'User Databases Transaction Log', 4 UNION ALL
		SELECT   16384, 'dbo.usp_hcCollectDatabaseDetails', 'Collect Database Details', NULL UNION ALL
		SELECT   32768, 'dbo.usp_hcCollectDiskSpaceUsage', 'Collect Disk Space Usage', NULL UNION ALL
		SELECT   65536, 'dbo.usp_hcCollectErrorlogMessages', 'Collect SQL Server errorlog Messages', NULL UNION ALL
		SELECT  131072, 'dbo.usp_hcCollectOSEventLogs', 'Collect OS Event Logs', NULL UNION ALL
		SELECT  262144, 'dbo.usp_hcCollectSQLServerAgentJobsStatus', 'Collect SQL Server Agent Jobs Status', NULL UNION ALL
		SELECT  524288, 'dbo.usp_hcCollectEventMessages', 'Collect Internal Event Messages', NULL UNION ALL
		SELECT 1048576, 'dbo.usp_monAlarmCustomReplicationLatency', 'Monitor Replication Latency', NULL UNION ALL
		SELECT 2097152, 'dbo.usp_monAlarmCustomSQLAgentFailedJobs', 'Monitor Failed SQL Server Agent Jobs', NULL UNION ALL
		SELECT 4194304, 'dbo.usp_monAlarmCustomTransactionsStatus', 'Monitor Transaction and Session Status', NULL
GO
