SET NOCOUNT ON
/*
*-----------------------------------------------------------------------------*
* dbaTDPMon (Troubleshoot Database Performance / Monitoring)                  *
* https://github.com/rentadba/dbaTDPMon, under GNU (GPLv3) licence model      *
*-----------------------------------------------------------------------------*
* Patch script: from version 2018.3 to 2018.4 (2018.04.11)					  *
*-----------------------------------------------------------------------------*
*/

/*---------------------------------------------------------------------------------------------------------------------*/
/* patch module: maintenance-plan																								   */
/*---------------------------------------------------------------------------------------------------------------------*/
RAISERROR('* Patch: 20180411-patch-upgrade-from-v2018_3-to-v2018_4-mp.sql', 10, 1) WITH NOWAIT

INSERT	INTO [dbo].[appInternalTasks] ([id], [descriptor], [task_name], [flg_actions])
		SELECT S.[id], S.[descriptor], S.[task_name], S.[flg_actions]
		FROM (
				SELECT       1 AS [id], 'dbo.usp_mpDatabaseConsistencyCheck' AS [descriptor], 'Database Consistency Check' AS [task_name], 1 AS [flg_actions] UNION ALL
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
				SELECT    8192, 'dbo.usp_mpDatabaseBackup', 'User Databases Transaction Log', 4
			)S
		LEFT JOIN [dbo].[appInternalTasks] ait ON S.[id] = ait.[id]
		WHERE ait.[id] IS NULL
GO
