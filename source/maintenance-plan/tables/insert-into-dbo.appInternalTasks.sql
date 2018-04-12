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
SET NOCOUNT ON
GO
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
