SET NOCOUNT ON
/*
*-----------------------------------------------------------------------------*
* dbaTDPMon (Troubleshoot Database Performance / Monitoring)                  *
* https://github.com/rentadba/dbaTDPMon, under GNU (GPLv3) licence model      *
*-----------------------------------------------------------------------------*
* Patch script: from version 2019.12 to 2020.01 (2019.12.12)				  *
*-----------------------------------------------------------------------------*
*/

/*---------------------------------------------------------------------------------------------------------------------*/
/* patch module: common																								   */
/*---------------------------------------------------------------------------------------------------------------------*/
RAISERROR('* Patch: 20191212-patch-upgrade-from-v2019_12-to-v2020_01-common.sql', 10, 1) WITH NOWAIT
IF NOT EXISTS(SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='dbo' AND TABLE_NAME='appInternalTasks' AND COLUMN_NAME='priority')
	begin
		EXEC ('ALTER TABLE [dbo].[appInternalTasks] ADD [priority] [tinyint] NULL');
	end
GO

UPDATE ait SET ait.[priority] = S.[priority]
FROM (
		SELECT   16384 AS [id], 'dbo.usp_hcCollectDatabaseDetails' AS [descriptor], 'Collect Database Details' AS [task_name], NULL AS [flg_actions], 1 AS [priority] UNION ALL
		SELECT   32768, 'dbo.usp_hcCollectDiskSpaceUsage', 'Collect Disk Space Usage', NULL, 2 AS [priority] UNION ALL
		SELECT   65536, 'dbo.usp_hcCollectErrorlogMessages', 'Collect SQL Server errorlog Messages', NULL, 3 AS [priority] UNION ALL
		SELECT  131072, 'dbo.usp_hcCollectOSEventLogs', 'Collect OS Event Logs', NULL, 4 AS [priority] UNION ALL
		SELECT  262144, 'dbo.usp_hcCollectSQLServerAgentJobsStatus', 'Collect SQL Server Agent Jobs Status', NULL, 5 AS [priority] UNION ALL
		SELECT  524288, 'dbo.usp_hcCollectEventMessages', 'Collect Internal Event Messages', NULL , 6 AS [priority]
	)S
INNER JOIN [dbo].[appInternalTasks] ait ON S.[id] = ait.[id];

UPDATE ait SET ait.[priority] = S.[priority]
FROM (
		SELECT       1 AS [id], 'dbo.usp_mpDatabaseConsistencyCheck' AS [descriptor], 'Database Consistency Check' AS [task_name], 1 AS [flg_actions], 1 AS [priority] UNION ALL
		SELECT       2, 'dbo.usp_mpDatabaseConsistencyCheck', 'Allocation Consistency Check', 12, 2 AS [priority] UNION ALL
		SELECT       4, 'dbo.usp_mpDatabaseConsistencyCheck', 'Tables Consistency Check', 2, 3 AS [priority] UNION ALL
		SELECT       8, 'dbo.usp_mpDatabaseConsistencyCheck', 'Reference Consistency Check', 16, 4 AS [priority] UNION ALL
		SELECT      16, 'dbo.usp_mpDatabaseConsistencyCheck', 'Perform Correction to Space Usage', 64, 5 AS [priority] UNION ALL
		SELECT      32, 'dbo.usp_mpDatabaseOptimize', 'Rebuild Heap Tables', 16, 6 AS [priority] UNION ALL
		SELECT      64, 'dbo.usp_mpDatabaseOptimize', 'Rebuild or Reorganize Indexes', 3, 7 AS [priority] UNION ALL
		SELECT     128, 'dbo.usp_mpDatabaseOptimize', 'Update Statistics', 8, 8 AS [priority] UNION ALL
		SELECT     256, 'dbo.usp_mpDatabaseShrink', 'Shrink Database (TRUNCATEONLY)', 2 , 9 AS [priority]UNION ALL
		SELECT     512, 'dbo.usp_mpDatabaseShrink', 'Shrink Log File', 1, 10 AS [priority] UNION ALL
		SELECT    1024, 'dbo.usp_mpDatabaseBackup', 'User Databases (diff)', 2, 12 AS [priority] UNION ALL
		SELECT    2048, 'dbo.usp_mpDatabaseBackup', 'User Databases (full)' , 1, 11 AS [priority] UNION ALL
		SELECT    4096, 'dbo.usp_mpDatabaseBackup', 'System Databases (full)', 1, 13 AS [priority] UNION ALL
		SELECT    8192, 'dbo.usp_mpDatabaseBackup', 'User Databases Transaction Log', 4, 14 AS [priority]
	)S
INNER JOIN [dbo].[appInternalTasks] ait ON S.[id] = ait.[id];
;

UPDATE ait SET ait.[priority] = S.[priority]
FROM (
		SELECT 1048576 AS [id], 'dbo.usp_monAlarmCustomReplicationLatency' AS [descriptor], 'Monitor Replication Latency' AS [task_name], NULL AS [flg_actions], 3 AS [priority] UNION ALL
		SELECT 2097152, 'dbo.usp_monAlarmCustomSQLAgentFailedJobs', 'Monitor Failed SQL Server Agent Jobs', NULL, 2 AS [priority] UNION ALL
		SELECT 4194304, 'dbo.usp_monAlarmCustomTransactionsStatus', 'Monitor Transaction and Session Status', NULL, 1 AS [priority] 
	)S
INNER JOIN [dbo].[appInternalTasks] ait ON S.[id] = ait.[id];
GO
